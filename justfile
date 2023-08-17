# Upgrade each of the given dependencies, or if no args are specified, then
# upgrade all dependencies.
upgrade *deps:
  go mod tidy
  [ -z "{{deps}}" ] && go get -u ./... || go get -u {{deps}}
  go mod tidy

# Lint all the things!
lint:
  yamlfmt -lint  # https://github.com/google/yamlfmt
  actionlint     # https://github.com/rhysd/actionlint
  yamllint .     # https://github.com/adrienverge/yamllint
  # Don't run if we're in CI in which case there's a dedicated action step.
  # https://github.com/golangci/golangci-lint
  echo "${CI:-}" | grep -qiE '^(1|y|yes|true)$' || golangci-lint run
