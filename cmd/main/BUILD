package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_binary", "py_library")

py_library(
    name = "main_lib",
    srcs = glob(
        ["*.py"],
        exclude = [
            "**/__pycache__/**",
            "**/*.pyc",
        ],
    ),
    deps = [
        "//pkg/common:common",
        "//internal/processors:processors",
        "//internal/monitors:monitors",
    ],
    imports = ["."],
)

# Main binary entry point
py_binary(
    name = "main",
    srcs = ["godcrazycode.py"],
    main = "godcrazycode.py",
    deps = [":main_lib"],
)

