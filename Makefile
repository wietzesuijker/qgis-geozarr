PLUGIN_NAME = qgis_geozarr
VERSION = $(shell grep '^version=' $(PLUGIN_NAME)/metadata.txt | cut -d= -f2)

zip:
	@rm -f $(PLUGIN_NAME).zip
	@cp LICENSE $(PLUGIN_NAME)/LICENSE
	zip -r $(PLUGIN_NAME).zip $(PLUGIN_NAME)/ \
		-x '*/__pycache__/*' '*.pyc' '*.pyo'
	@rm $(PLUGIN_NAME)/LICENSE
	@echo "Built $(PLUGIN_NAME).zip (v$(VERSION))"

test:
	python -m pytest tests/ -q

lint:
	ruff check $(PLUGIN_NAME)/ tests/

clean:
	rm -f $(PLUGIN_NAME).zip
	find $(PLUGIN_NAME) -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

.PHONY: zip test lint clean
