10.128.0.8 master-node
10.128.0.9 worker-node-01

sudo su
yum update

swapoff -a  
# go to fstab comment out the line with /swapfile
nano /etc/fstab

# change hostname restart the vm
nano /etc/hostname

apt-get install openssh-server

apt-get install -y docker.io

yum update && yum install -y apt-transport-https curl

cat <<EOF > /etc/yum.repos.d/kubernetes.repo
[kubernetes]
name=Kubernetes
baseurl=https://packages.cloud.google.com/yum/repos/kubernetes-el6-x86_64
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=https://packages.cloud.google.com/yum/doc/yum-key.gpg https://packages.cloud.google.com/yum/doc/rpm-package-key.gpg
EOF

apt-get update

sudo yum install -y kubelet kubeadm kubectl

nano /etc/systemd/system/kubelet.service.d/10-kubeadm.conf
# add this: Enviroment="cgroup-driver=systemd/cgroup-driver=cgroupfs"

kubeadm init --pod-network-cidr=192.168.0.0/16 --apiserver-advertise-address=10.128.0.16

To start using your cluster, you need to run the following as a regular user:
  mkdir -p $HOME/.kube
  sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
  sudo chown $(id -u):$(id -g) $HOME/.kube/config

kubectl get pods -o wide --all-namespaces

kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.0.0/aio/deploy/recommended.yaml

kubectl proxy

http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/
#http://localhost:8001/api/v1/namespaces/kube-system/services/https:kubernetes-dashboard:/proxy/#!/login

kubectl create serviceaccount dashboard -n default

kubectl create clusterrolebinding dashboard-admin -n default \
--clusterrole=cluster-admin \
--serviceaccount=default:dashboard

kubectl get secret $(kubectl get serviceaccount dashboard -o jsonpath="{.secrets[0].name}") -o jsonpath="{.data.token}" | base64 --decode > sk

kubeadm join 10.128.0.16:6443 --token kcg667.c3cy45xy1his1q9i \
    --discovery-token-ca-cert-hash sha256:e0c7018a8c3e76b57c33ea44175d6951ebc639f5f384e7ecd86f7c979a3e080e
