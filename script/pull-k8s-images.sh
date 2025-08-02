#!/bin/bash
# 一键拉取 Kubernetes v1.32.4 及常用依赖镜像（适配国内环境）
# 适用于 Docker Desktop/kubeadm 本地开发
# 可根据实际版本修改变量

# 最新 Kubernetes 版本（2025年8月官方最新稳定版）
K8S_VERSION=v1.32.4
PAUSE_VERSION=3.10
ETCD_VERSION=3.5.13-0
COREDNS_VERSION=v1.11.1

# 镜像源
REGISTRY=registry.aliyuncs.com/google_containers

# 拉取并打 tag
images=(
  kube-apiserver:${K8S_VERSION}
  kube-controller-manager:${K8S_VERSION}
  kube-scheduler:${K8S_VERSION}
  kube-proxy:${K8S_VERSION}
  pause:${PAUSE_VERSION}
  etcd:${ETCD_VERSION}
  coredns:${COREDNS_VERSION}
)

echo "拉取 Kubernetes 相关镜像..."
for image in "${images[@]}"; do
  src_img="$REGISTRY/${image}"
  dst_img="k8s.gcr.io/${image}"
  if [[ $image == coredns* ]]; then
    dst_img="k8s.gcr.io/coredns/${image#coredns:}"
  fi
  echo "拉取 $src_img 并打 tag $dst_img"
  docker pull $src_img
  docker tag $src_img $dst_img
  docker rmi $src_img
  echo "完成 $dst_img"
done

echo "所有 Kubernetes 依赖镜像已拉取并打 tag 完成！"
