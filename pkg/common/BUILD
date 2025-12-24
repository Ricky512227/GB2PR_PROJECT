package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_library")

py_library(
    name = "common",
    srcs = glob(
        ["*.py"],
        exclude = [
            "**/__pycache__/**",
            "**/*.pyc",
        ],
    ),
    deps = [],
    imports = ["."],
)

