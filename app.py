#!/usr/bin/env python3
from aws_cdk import core as cdk

from infra.service_catalog_stack import ServiceCatalogStack

app = cdk.App()
synth = cdk.DefaultStackSynthesizer(
    generate_bootstrap_version_rule=False,
)
ServiceCatalogStack(app, "ServiceCatalogProjectStack", synthesizer=synth)

app.synth()
