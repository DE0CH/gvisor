load("//tools:defs.bzl", "go_library", "go_test")

package(
    default_applicable_licenses = ["//:license"],
    licenses = ["notice"],
)

go_library(
    name = "precopy",
    srcs = ["precopy.go"],
    visibility = ["//:sandbox"],
    deps = [
        "//pkg/sentry/checkpoint",
        "//pkg/sentry/pgalloc",
        "//pkg/sentry/state/checkpointfiles",
        "//pkg/sync",
        "@org_golang_x_sys//unix:go_default_library",
    ],
)

go_test(
    name = "precopy_test",
    size = "small",
    srcs = ["precopy_test.go"],
    embed = [":precopy"],
    deps = [
        "//pkg/sentry/checkpoint",
        "//pkg/sentry/pgalloc",
        "//pkg/sentry/state/checkpointfiles",
    ],
)
