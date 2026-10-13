#!/usr/bin/env bash
# currency reports every dependency, toolchain, and pin that trails its latest
# release, one line each, and exits non-zero when it reports any. It reads the
# network and writes nothing. Run it through `mise run currency`.
#
# Each scan captures its command's output in a variable before reading it, so
# under set -e a scan command that fails (a go list missing a go.sum entry)
# fails currency instead of reporting nothing.
set -euo pipefail
cd "$MISE_PROJECT_ROOT"
export GOWORK=off

stale=0
report() {
	echo "$1"
	stale=1
}

# matches prints each match of a grep pattern in the given files that exist.
# A missing file or no match is no output; any other grep error fails.
matches() {
	local pattern=$1 file
	local files=()
	shift
	for file in "$@"; do
		if [ -e "$file" ]; then files+=("$file"); fi
	done
	[ ${#files[@]} -gt 0 ] || return 0
	grep -ho "$pattern" "${files[@]}" || [ $? -eq 1 ]
}

# tool_modules prints the module of each package a tool directive in the
# current directory's module declares, once each. Only go list's standard
# output names modules: its standard error carries the warning when no tool
# directive matches and the progress of any download, so it is shown only
# when go list fails.
tool_modules() {
	local out err
	err=$(mktemp)
	if ! out=$(go list -f '{{.Module.Path}}' tool 2>"$err"); then
		cat "$err" >&2
		rm -f "$err"
		return 1
	fi
	rm -f "$err"
	[ -z "$out" ] || sort -u <<<"$out"
}

go_minor=$(mise latest go | cut -d. -f1,2)

for mod in $GO_MODULES; do
	# Direct requirements with a newer version within their major.
	updates=$(cd "$mod" && go list -m -u -f \
		'{{if and (not .Main) (not .Indirect) .Update}}{{.Path}} {{.Version}} -> {{.Update.Version}}{{end}}' all)
	while read -r line; do
		[ -n "$line" ] && report "$mod/go.mod: $line"
	done <<<"$updates"

	# Tool modules with a newer version within their major. A tool's module is
	# an indirect requirement, which the scan above leaves out; one the module
	# also requires directly is reported there, so it is skipped here.
	tools=$(cd "$mod" && tool_modules)
	if [ -n "$tools" ]; then
		updates=$(cd "$mod" && go list -m -u -f \
			'{{if and (not .Main) .Indirect .Update}}{{.Path}} {{.Version}} -> {{.Update.Version}}{{end}}' $tools)
		while read -r line; do
			[ -n "$line" ] && report "$mod/go.mod: $line"
		done <<<"$updates"
	fi

	# The go directive's minor against the current Go minor.
	directive=$(cd "$mod" && go mod edit -json | jq -r .Go | cut -d. -f1,2)
	[ "$directive" = "$go_minor" ] || report "$mod/go.mod: go $directive -> go $go_minor"
done

# mise tools pinned below their latest version.
outdated=$(mise outdated --bump --local --json |
	jq -r 'to_entries[] | "\(.key) \(.value.requested) -> \(.value.bump)"')
while read -r line; do
	[ -n "$line" ] && report "mise.toml: $line"
done <<<"$outdated"

# The workflows of the project root and of the git top-level, de-duplicated.
# They are one directory when the project is the repository, and two when the
# repository nests the project in a subdirectory, as the template repository
# does. A project not yet in git has only its own.
root=$(pwd -P)
top=$(git rev-parse --show-toplevel 2>/dev/null || echo "$root")
workflows=("$root"/.github/workflows/*.yml)
[ "$top" = "$root" ] || workflows+=("$top"/.github/workflows/*.yml)

# GitHub Actions pinned below the action's latest release.
actions=$(matches 'uses: *[^ ]*@[^ ]*' "${workflows[@]}" | sed 's/uses: *//' | sort -u)
for uses in $actions; do
	action=${uses%@*}
	pin=${uses#*@}
	repo=$(echo "$action" | cut -d/ -f1,2)
	latest=$(gh api "repos/$repo/releases/latest" --jq .tag_name)
	[ "$pin" = "$latest" ] || report ".github/workflows: $action $pin -> $latest"
done

# latest_tag prints the highest semver tag of an image that carries the given
# pin's variant suffix (18.6-alpine -> -alpine). A tag needs at least one dot
# to count: some images also publish bare build numbers (Grafana's 98813352)
# that would otherwise sort above every release.
latest_tag() {
	local image=$1 pin=$2 suffix pattern latest
	suffix=${pin#"${pin%%[!0-9.]*}"}
	pattern="^[0-9]+(\.[0-9]+)+${suffix//./\\.}\$"
	latest=$(crane ls "$image" | grep -E "$pattern" | sed "s/${suffix}\$//" | sort -V | tail -n1) || return
	echo "$latest$suffix"
}

# Compose and CI images pinned on an image: line below their latest. A
# compose service that builds from compose/<service>/Dockerfile has no image:
# line; its pin is the Dockerfile's FROM line, scanned below.
images=$(matches '^ *image: *[^ ]*' compose.yml compose/*.yml "${workflows[@]}" |
	sed 's/^ *image: *//' | tr -d "\"'" | sort -u)
for ref in $images; do
	image=${ref%:*}
	pin=${ref##*:}
	latest=$(latest_tag "$image" "$pin")
	[ "$pin" = "$latest" ] || report "images: $image $pin -> $latest"
done

# Compose services' Dockerfiles pinned on their FROM line below the latest.
for dockerfile in compose/*/Dockerfile; do
	[ -e "$dockerfile" ] || continue
	ref=$(sed -n 's/^FROM  *\([^ ]*\).*/\1/p' "$dockerfile" | head -n1)
	image=${ref%:*}
	pin=${ref##*:}
	latest=$(latest_tag "$image" "$pin")
	[ "$pin" = "$latest" ] || report "$dockerfile: $image $pin -> $latest"
done

exit "$stale"
