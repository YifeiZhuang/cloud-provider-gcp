package main

import (
	"context"
	"fmt"
	cloudcontrollerconfig "k8s.io/cloud-provider/app/config"
	cloudprovider "k8s.io/cloud-provider"
	genericcontrollermanager "k8s.io/controller-manager/app"

	"k8s.io/cloud-provider/app"
	"k8s.io/klog/v2"
	"k8s.io/cloud-provider-gcp/pkg/controller/dynamicpod"
	"k8s.io/cloud-provider-gcp/providers/gce"
	"k8s.io/controller-manager/controller"
)

const (
	aliasIPControllerName = "dynamic-pod-controller"
	aliasIPControllerWorkers = 2 
)

func startDynamicPodIPControllerWrapper(initCtx app.ControllerInitContext, config *cloudcontrollerconfig.CompletedConfig, c cloudprovider.Interface) app.InitFunc {
	return func(ctx context.Context, controllerCtx genericcontrollermanager.ControllerContext) (controller.Interface, bool, error) {
		return startDynamicPodIPController(ctx, controllerCtx, c)
	}
}

// startDynamicPodIPController is the initialization function for the DynamicPodIPController.
func startDynamicPodIPController(ctx context.Context, controllerCtx genericcontrollermanager.ControllerContext, cloud cloudprovider.Interface) (controller.Interface, bool, error) {
	klog.Infof("Starting %s", aliasIPControllerName)

	// 1. Get KubeClient
	kubeClient := controllerCtx.ClientBuilder.ClientOrDie(aliasIPControllerName)

	// 2. Get NodeInformer
	nodeInformer := controllerCtx.InformerFactory.Core().V1().Nodes()

	// 3. Get GCP Project ID
	// The Cloud provider interface needs to be cast to the specific GCP type.
	gceCloud, ok := cloud.(*gce.Cloud)
	if !ok {
		return nil, false, fmt.Errorf("%s: failed to get GCP cloud provider specifics: cloud provider is not the expected GCP type", aliasIPControllerName)
	}

	// 4. Create the AliasIPController instance
	// Note: The NewAliasIPController function is from your dynamic_pod_sync.go
	dynamicpod, err := dynamicpod.NewDynamicPodIPController(
		kubeClient,
		nodeInformer,
		gceCloud,
	)
	if err != nil {
		return nil, false, fmt.Errorf("%s: failed to create: %w", aliasIPControllerName, err)
	}

	// 5. Start the controller in a goroutine
	// The controller's Start method is blocking and manages its own workers.
	go func() {
		if err := dynamicpod.Start(ctx, aliasIPControllerWorkers); err != nil {
			klog.Errorf("%s: failed to run: %v", aliasIPControllerName, err)
			// Depending on requirements, you might want to trigger a process exit or other alerting here.
			// For now, we log the error. The main CCM loop might handle fatal errors.
		}
		klog.Infof("%s: stopped", aliasIPControllerName)
	}()

	klog.Infof("%s: started successfully", aliasIPControllerName)
	return dynamicpod, true, nil
}