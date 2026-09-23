# Security policy

## Supported versions

tempdes is pre-1.0 and has no releases yet. Security fixes land on the `main` branch.

## Reporting a vulnerability

Please don't report security problems in public issues. Use GitHub's
[private vulnerability reporting](https://github.com/iw/tempdes/security/advisories/new)
instead: go to the **Security** tab and choose **Report a vulnerability**.

Please include:

* what the problem is and what an attacker could do with it;
* the steps or input files to reproduce it;
* the tempdes version or commit you used.

The maintainer will acknowledge your report as soon as possible, and will keep you informed
while working on a fix and advisory.

## Scope

tempdes runs offline. It reads the scenario, dynamic config, Helm values and metrics files you
give it, and writes reports. It doesn't connect to Temporal, Kubernetes or the network.

These count as security issues:

* malicious input that crashes, hangs or exhausts memory in ways a size limit can't prevent;
* HTML reports that execute script embedded in input files;
* anything that writes outside the output paths you asked for.

Treat scenario files like code:

* a scenario can reference other files on your machine (`dynamic_config_files`, `helm_values`,
  `calibration.observations`);
* a scenario can request an arbitrarily large simulation.

Only run scenarios you trust, or run them with resource limits.
