#!/usr/bin/env groovy

dockerfile {
    slackChannel = ''
    // upstreamProjects = 'confluentinc/schema-registry'
    extraDeployArgs = '-Ddocker.skip=true'
    dockerPush = false
    dockerScan = false
    dockerImageClean = false
    testbreakReporting = false
    downStreamRepos = ["confluent-security-plugins", "confluent-cloud-plugins"]
    nanoVersion = true
}
