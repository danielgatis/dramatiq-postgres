default:

RST=$(wildcard *.rst)
check:
	set -eux; for rst in $(RST) ; do \
		$$VIRTUAL_ENV/bin/rst2html.py --strict $$rst >/dev/null; \
	done
