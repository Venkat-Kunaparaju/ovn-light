// SPDX-License-Identifier: Apache-2.0
package kube

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/trozet/ovn-light/internal/model"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

type Client struct {
	Core    kubernetes.Interface
	Dynamic dynamic.Interface
	Config  *rest.Config
}

func New(kubeconfig, contextName string) (*Client, error) {
	var config *rest.Config
	var err error
	if kubeconfig == "" && contextName == "" {
		config, err = rest.InClusterConfig()
	}
	if config == nil {
		rules := clientcmd.NewDefaultClientConfigLoadingRules()
		if kubeconfig != "" {
			rules.ExplicitPath = kubeconfig
		}
		config, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{CurrentContext: contextName}).ClientConfig()
	}
	if err != nil {
		return nil, err
	}
	config = rest.CopyConfig(config)
	config.QPS = 20
	config.Burst = 40
	config.UserAgent = "ovn-light"
	config.Timeout = 30 * time.Second
	c, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	d, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &Client{Core: c, Dynamic: d, Config: config}, nil
}

var Resources = map[string]schema.GroupVersionResource{
	"userdefinednetworks":            {Group: "k8s.ovn.org", Version: "v1", Resource: "userdefinednetworks"},
	"clusteruserdefinednetworks":     {Group: "k8s.ovn.org", Version: "v1", Resource: "clusteruserdefinednetworks"},
	"clusternetworkconnects":         {Group: "k8s.ovn.org", Version: "v1", Resource: "clusternetworkconnects"},
	"network-attachment-definitions": {Group: "k8s.cni.cncf.io", Version: "v1", Resource: "network-attachment-definitions"},
	"adminnetworkpolicies":           {Group: "policy.networking.k8s.io", Version: "v1alpha1", Resource: "adminnetworkpolicies"},
	"baselineadminnetworkpolicies":   {Group: "policy.networking.k8s.io", Version: "v1alpha1", Resource: "baselineadminnetworkpolicies"},
	"egressfirewalls":                {Group: "k8s.ovn.org", Version: "v1", Resource: "egressfirewalls"},
	"multi-networkpolicies":          {Group: "k8s.cni.cncf.io", Version: "v1beta1", Resource: "multi-networkpolicies"},
}

func (c *Client) Inventory(ctx context.Context) (model.Inventory, error) {
	i := model.Inventory{}
	p, err := c.Core.CoreV1().Pods("").List(ctx, meta.ListOptions{})
	if err != nil {
		return i, fmt.Errorf("list pods: %w", err)
	}
	i.Pods = p.Items
	n, err := c.Core.CoreV1().Namespaces().List(ctx, meta.ListOptions{})
	if err != nil {
		return i, fmt.Errorf("list namespaces: %w", err)
	}
	i.Namespaces = n.Items
	np, err := c.Core.NetworkingV1().NetworkPolicies("").List(ctx, meta.ListOptions{})
	if err != nil {
		return i, fmt.Errorf("list network policies: %w", err)
	}
	i.Policies = np.Items
	s, err := c.Core.CoreV1().Services("").List(ctx, meta.ListOptions{})
	if err != nil {
		return i, fmt.Errorf("list services: %w", err)
	}
	i.Services = s.Items
	i.Resources = make(map[string][]unstructured.Unstructured)
	for name, gvr := range Resources {
		list, err := c.Dynamic.Resource(gvr).List(ctx, meta.ListOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			i.Errors = append(i.Errors, fmt.Sprintf("read %s: %v", name, err))
			continue
		}
		i.Resources[name] = list.Items
	}
	c.diagnosticInventory(ctx, &i)
	return i, nil
}

type Target struct{ Namespace, Pod, Container string }

type limitedBuffer struct {
	buf       bytes.Buffer
	remaining int
}

func (b *limitedBuffer) String() string { return b.buf.String() }

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.remaining {
		return 0, fmt.Errorf("command output exceeds size limit")
	}
	b.remaining -= len(p)
	return b.buf.Write(p)
}

func (c *Client) Exec(ctx context.Context, t Target, args []string, stdin io.Reader) (string, error) {
	return c.execBounded(ctx, t, args, stdin, 32<<20)
}

func (c *Client) execBounded(ctx context.Context, t Target, args []string, stdin io.Reader, limit int) (string, error) {
	if len(args) >= 3 && args[0] == "ovs-appctl" && args[1] == "-t" && args[2] == "ovs-vswitchd" {
		args = append([]string{"sh", "-c", `pid=$(cat /var/run/openvswitch/ovs-vswitchd.pid) || exit
case "$pid" in ''|*[!0-9]*) exit 1;; esac
exec ovs-appctl -t "/var/run/openvswitch/ovs-vswitchd.$pid.ctl" "$@"`, "ovs-appctl"}, args[3:]...)
	}
	req := c.Core.CoreV1().RESTClient().Post().Resource("pods").Name(t.Pod).Namespace(t.Namespace).SubResource("exec").VersionedParams(&core.PodExecOptions{Container: t.Container, Command: args, Stdin: stdin != nil, Stdout: true, Stderr: true}, scheme.ParameterCodec)
	config := rest.CopyConfig(c.Config)
	config.Timeout = 0
	executor, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return "", err
	}
	out := &limitedBuffer{remaining: limit}
	stderr := &limitedBuffer{remaining: 1 << 20}
	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdin: stdin, Stdout: out, Stderr: stderr})
	if err != nil {
		return "", fmt.Errorf("%s/%s[%s] %s: %w: %s", t.Namespace, t.Pod, t.Container, args[0], err, strings.TrimSpace(stderr.String()))
	}
	return out.String(), nil
}
