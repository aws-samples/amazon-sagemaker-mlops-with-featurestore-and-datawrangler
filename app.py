#!/usr/bin/env python3
from aws_cdk import App, DefaultStackSynthesizer

from infra.service_catalog_stack import ServiceCatalogStack

app = App()
synth = DefaultStackSynthesizer(
    generate_bootstrap_version_rule=False,
)
ServiceCatalogStack(app, "ServiceCatalogProjectStack", synthesizer=synth)

app.synth()
