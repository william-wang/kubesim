package cm

import (
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/kubelet/cm"

	simulatorconfig "volcano.sh/kubesim/pkg/config"
)

type fakeContainerManager struct {
	cm.ContainerManager
	resourcesList v1.ResourceList
}

// NewFakeContainerManager return a fake container manager with fake device plugins.
func NewFakeContainerManager(nc *simulatorconfig.NodeClasses) cm.ContainerManager {
	cm := &fakeContainerManager{
		ContainerManager: cm.NewStubContainerManager(),
		resourcesList:    make(v1.ResourceList),
	}
	for name, value := range nc.Resources.Capacity {
		if helper.IsNativeResource(v1.ResourceName(name)) {
			klog.InfoS("Skip register native resource", "name", name)
			continue
		}
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			klog.ErrorS(err, "Failed to parse resource value", "name", name, "value", value)
			continue
		}
		cm.resourcesList[v1.ResourceName(name)] = *resource.NewQuantity(i, resource.DecimalSI)
	}
	return cm
}

func (f *fakeContainerManager) GetDevicePluginResourceCapacity() (v1.ResourceList, v1.ResourceList, []string) {
	return f.resourcesList, f.resourcesList, []string{}
}
