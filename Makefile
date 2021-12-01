CC = clang
CFLAGS = -O3 -flto -Werror -Walloca -Wextra -Wcast-qual -Wconversion -Wformat=2 -Wformat-security -Wnull-dereference -Wstack-protector -Wvla -Warray-bounds -Warray-bounds-pointer-arithmetic -Wassign-enum -Wbad-function-cast -Wconditional-uninitialized -Wconversion -Wfloat-equal -Wformat-type-confusion -Widiomatic-parentheses -Wimplicit-fallthrough -Wloop-analysis -Wpointer-arith -Wshift-sign-overflow -Wshorten-64-to-32 -Wswitch-enum -Wtautological-constant-in-range-compare -Wunreachable-code-aggressive -Wthread-safety -Wthread-safety-beta -Wcomma -D_FORTIFY_SOURCE=2 -fstack-protector-strong -fsanitize=safe-stack -fPIE -fstack-clash-protection -Wl,-z,relro -Wl,-z,now -Wl,-z,noexecstack -Wl,-z,separate-code
LDFLAGS = -lzstd -lpthread

omzstd: omzstd.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o omzstd omzstd.c



.PHONY: clean
clean:
	rm -f $(obj) omzstd