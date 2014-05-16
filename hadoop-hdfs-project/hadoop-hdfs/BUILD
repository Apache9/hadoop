cc_library(
  name = "libhdfs",
  srcs = [
    "src/main/native/libhdfs/exception.c",
    "src/main/native/libhdfs/hdfs.c",
    "src/main/native/libhdfs/jni_helper.c",
  ],
  incs = [
    "/opt/soft/jdk/include",
    "/opt/soft/jdk/include/linux",
  ],
  extra_cppflags = [
    "-Wno-return-type",
    "-Wno-sign-compare",
    "-Wno-unused-variable"
  ],
)
