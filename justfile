# upgrade all dependencies
upgrade *deps:
  go mod tidy
  [ -z "{{deps}}" ] && go get -u ./... || go get -u {{deps}}
  go mod tidy

lint:
  echo "${CI:-}" | grep -qiE '^(1|y|yes|true)$' || golangci-lint run
  yamllint .
  yamlfmt -lint
