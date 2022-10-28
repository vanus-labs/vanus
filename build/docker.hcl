# docker.hcl

group "dev" {
  targets = ["controller", "gateway", "store", "timer", "trigger"]
}

variable "TAG" {
  default = "dev"
}

target "controller" {
  dockerfile = "./images/controller/Dockerfile"
  tags = ["public.ecr.aws/vanus/controller:${TAG}"]
  platforms = ["linux/amd64", "linux/arm64"]
}

target "gateway" {
  dockerfile = "images/gateway/Dockerfile"
  tags = ["public.ecr.aws/vanus/gateway:${TAG}"]
  platforms = ["linux/amd64", "linux/arm64"]
}

target "store" {
  dockerfile = "images/store/Dockerfile"
  tags = ["public.ecr.aws/vanus/store:${TAG}"]
  platforms = ["linux/amd64", "linux/arm64"]
}

target "timer" {
  dockerfile = "images/timer/Dockerfile"
  tags = ["public.ecr.aws/vanus/timer:${TAG}"]
  platforms = ["linux/amd64", "linux/arm64"]
}

target "trigger" {
  dockerfile = "images/trigger/Dockerfile"
  tags = ["public.ecr.aws/vanus/trigger:${TAG}"]
  platforms = ["linux/amd64", "linux/arm64"]
}