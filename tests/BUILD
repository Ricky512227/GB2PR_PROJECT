package(default_visibility = ["//visibility:public"])

load("@rules_python//python:defs.bzl", "py_test")

py_test(
    name = "test_package_import",
    srcs = ["test_package_import.py"],
    deps = [
        "//pkg/common:common",
    ],
    imports = ["."],
)
