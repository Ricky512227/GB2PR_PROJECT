package(default_visibility = ["//internal/monitors:__subpackages__"])

load("@rules_python//python:defs.bzl", "py_library")

py_library(
    name = "monitors",
    srcs = glob(
        ["*.py"],
        exclude = [
            "**/__pycache__/**",
            "**/*.pyc",
        ],
    ),
    deps = [
        "//pkg/common:common",
    ],
    imports = ["."],
)

