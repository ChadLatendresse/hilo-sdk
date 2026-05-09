module github.com/ChadLatendresse/hilo-sdk

go 1.26

toolchain go1.26.3

retract (
	v0.1.0 // Contains personal data (real account/device ids); use v0.1.3+.
	v0.1.1 // Contains personal data (real account/device ids); use v0.1.3+.
	v0.1.2 // Contains personal data (real account/device ids); use v0.1.3+.
)

require github.com/coder/websocket v1.8.14
