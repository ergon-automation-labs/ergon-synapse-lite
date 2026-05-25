.PHONY: help deps test credo dialyzer coverage check format clean release publish-release \
	watch-pi-go-trigger-decisions watch-pi-go-trigger-skips watch-pi-go-events pi-go-status pi-go-dashboard \
	smoke-factory-decision-loop push-and-publish deploy-prod \
	suggestion-report suggestion-watch \
	repo-map repo-map-check

MIX ?= /Users/abby/.local/share/mise/shims/mix

NATS_BOX_IMAGE := natsio/nats-box:latest
NATS_URL := nats://$${NATS_HOST:-host.docker.internal}:$${NATS_PORT:-4222}

help:
	@echo "bot_army_synapse - Make targets"
	@echo ""
	@echo "Digests & Reports:"
	@echo "  make suggestion-report  - Trigger GTD/feeds/calendar/jobs digest generation now"
	@echo "  make suggestion-watch   - Watch for digest generation (subscribe to events)"
	@echo ""
	@echo "Pi-Go:"
	@echo "  make watch-pi-go-trigger-decisions - Stream Synapse pi-go trigger decisions"
	@echo "  make watch-pi-go-trigger-skips     - Stream only skipped trigger decisions"
	@echo "  make watch-pi-go-events            - Stream pi-go lifecycle events (started/progress/completed/failed)"
	@echo "  make pi-go-status                  - Snapshot of recent scheduler decision and pi-go event"
	@echo "  make pi-go-dashboard               - Live dashboard: scheduler decisions + pi-go lifecycle events"
	@echo "  make smoke-factory-decision-loop   - Publish vote/evidence and print decision output"
	@echo ""
	@echo "Release:"
	@echo "  make release          - Build OTP release (runs test first; same gate as pre-push)"
	@echo "  make publish-release  - Build, tarball, GitHub release (synapse)"
	@echo "  make push-and-publish - git push then publish-release"
	@echo ""
	@echo "Dev:"
	@echo "  make test / credo / dialyzer / check / format / clean"
	@echo ""
	@echo "Docs:"
	@echo "  make repo-map         - Regenerate architecture/repo-map.md from code"
	@echo "  make repo-map-check   - Check if repo-map.md is stale"

deps:
	$(MIX) deps.get

test:
	@echo "Running test suite (37 test files)..."
	@echo "Expected: 3-5 minutes (previously 25+ min before optimizations)"
	@echo ""
	@time $(MIX) test

credo:
	$(MIX) credo --only warning

dialyzer: deps
	$(MIX) dialyzer

coverage:
	$(MIX) coveralls

check: test credo
	@echo "All checks passed!"

format:
	$(MIX) format

clean:
	$(MIX) clean
	rm -rf _build cover
	rm -rf synapse-*.tar.gz

release: test
	@echo "==============================================="
	@echo "Building OTP release"
	@echo "==============================================="
	rm -rf _build/prod/rel/synapse
	MIX_ENV=prod $(MIX) release
	@echo ""
	@echo "✓ Release built successfully"
	@echo "Location: _build/prod/rel/synapse/"
	@echo ""

publish-release: release
	@echo "==============================================="
	@echo "Publishing release to GitHub"
	@echo "==============================================="
	@echo ""
	@bash -c 'set -e; \
	VERSION=$$(if [ -f _build/prod/rel/synapse/releases/start_erl.data ]; then awk "{print \$$2}" _build/prod/rel/synapse/releases/start_erl.data; else tail -1 _build/prod/rel/synapse/releases/RELEASES | cut -d" " -f2; fi); \
	if [ -z "$$VERSION" ]; then echo "Could not read release version"; exit 1; fi; \
	echo "[1/3] Version: $$VERSION"; \
	echo "[2/3] Creating tarball (synapse-$$VERSION.tar.gz)..."; \
	tar -czf synapse-$$VERSION.tar.gz -C _build/prod/rel synapse/; \
	echo "[3/3] Publishing to GitHub..."; \
	gh release create v$$VERSION synapse-$$VERSION.tar.gz \
		--title "Release v$$VERSION" \
		--notes "Synapse Elixir release v$$VERSION. Download and deploy with Jenkins." \
		--draft=false; \
	echo ""; \
	echo "✓ Release v$$VERSION published successfully"; \
	rm synapse-$$VERSION.tar.gz; \
	echo "Timeline: test (~3-5min) → build release (~1min) → publish (~1min)"; \
	echo ""'

watch-pi-go-trigger-decisions:
	@echo "Subscribing to events.synapse.pi_go.trigger.decision on $${NATS_HOST:-host.docker.internal}:$${NATS_PORT:-4222}"
	docker run --rm -it -e NATS_HOST -e NATS_PORT $(NATS_BOX_IMAGE) \
	  sh -lc "nats sub -s $(NATS_URL) 'events.synapse.pi_go.trigger.decision'"

watch-pi-go-trigger-skips:
	@echo "Subscribing to skipped decisions on events.synapse.pi_go.trigger.decision"
	docker run --rm -it -e NATS_HOST -e NATS_PORT $(NATS_BOX_IMAGE) \
	  sh -lc "nats sub -s $(NATS_URL) 'events.synapse.pi_go.trigger.decision' | grep -E '\"status\"[[:space:]]*:[[:space:]]*\"skipped\"'"

watch-pi-go-events:
	@echo "Subscribing to pi-go lifecycle events on pi-go.event.> on $${NATS_HOST:-localhost}:$${NATS_PORT:-4222}"
	nats sub --server nats://$${NATS_HOST:-localhost}:$${NATS_PORT:-4222} 'pi-go.event.>'

pi-go-status:
	@echo "--- Recent pi-go scheduler decision ---"
	@nats sub --server nats://$${NATS_HOST:-localhost}:$${NATS_PORT:-4222} 'events.synapse.pi_go.trigger.decision' --count 1 --timeout 5 2>&1 || echo "(none)"
	@echo ""
	@echo "--- Recent pi-go lifecycle event ---"
	@nats sub --server nats://$${NATS_HOST:-localhost}:$${NATS_PORT:-4222} 'pi-go.event.>' --count 1 --timeout 5 2>&1 || echo "(none)"

pi-go-dashboard:
	@echo "Starting pi-go dashboard (Ctrl-C to stop)..."
	@nats sub --server nats://$${NATS_HOST:-localhost}:$${NATS_PORT:-4222} 'events.synapse.pi_go.trigger.decision' & \
	  nats sub --server nats://$${NATS_HOST:-localhost}:$${NATS_PORT:-4222} 'pi-go.event.>' & \
	  wait

suggestion-report:
	@echo "Suggestion reports are generated every 6 hours (default) by SuggestionReporter."
	@echo "Reports:"
	@echo "  - ~/Documents/personal_os/inbox/gtd-items.md (GTD overview + all active tasks)"
	@echo "  - ~/Documents/personal_os/inbox/context.md (articles, calendar, jobs, health)"
	@echo ""
	@echo "Last modification:"
	@ls -lh ~/Documents/personal_os/inbox/gtd-items.md ~/Documents/personal_os/inbox/context.md 2>/dev/null || echo "Reports not yet generated. Run 'make suggestion-watch' to monitor generation."

suggestion-watch:
	@echo "Monitoring suggestion report files (Ctrl-C to stop)..."
	@echo "Watching: ~/Documents/personal_os/inbox/{gtd-items.md,context.md}"
	@echo ""
	@while true; do \
		if [ -f ~/Documents/personal_os/inbox/gtd-items.md ]; then \
			echo "[GTD Items - Last updated: $$(stat -f '%Sm' ~/Documents/personal_os/inbox/gtd-items.md)]"; \
			head -20 ~/Documents/personal_os/inbox/gtd-items.md; \
		fi; \
		echo ""; \
		if [ -f ~/Documents/personal_os/inbox/context.md ]; then \
			echo "[Context - Last updated: $$(stat -f '%Sm' ~/Documents/personal_os/inbox/context.md)]"; \
			head -20 ~/Documents/personal_os/inbox/context.md; \
		fi; \
		echo ""; \
		echo "Press Ctrl-C to stop. Checking again in 30s..."; \
		sleep 30; \
	done

smoke-factory-decision-loop:
	@bash scripts/factory_decision_smoke.sh

push-and-publish:
	@git push && $(MAKE) publish-release

repo-map:
	@echo "Regenerating architecture/repo-map.md..."
	@bash scripts/generate_repo_map.sh
	@echo "Done."

repo-map-check:
	@bash scripts/generate_repo_map.sh --check
