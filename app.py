#!/usr/bin/env python3
from aws_cdk import App, DefaultStackSynthesizer

from infra.service_catalog_stack import ServiceCatalogStack
from infra.service_catalog_stack_no_artifacts import ServiceCatalogStackNoArtifacts

app = App()

synth = DefaultStackSynthesizer(
    generate_bootstrap_version_rule=False,
)

StackToDeploy = ServiceCatalogStack
if app.node.try_get_context("NoArtifacts"):
    StackToDeploy = ServiceCatalogStackNoArtifacts


StackToDeploy(
    app,
    "MLOpsCustomTemplate",
    synthesizer=synth,
)

app.synth()
