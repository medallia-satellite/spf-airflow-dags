#!/usr/bin/env groovy

pipeline {
    agent {
        label 'general'
    }

    options {
        ansiColor('xterm')
    }

    stages {
        stage('Unit tests') {
            steps {
                sh 'python3 --version'
                sh 'python3 -m venv .venv'
                sh '.venv/bin/pip install --upgrade pip'
                sh '.venv/bin/pip install -r requirements-test.txt'
                sh '.venv/bin/python -m pytest -o junit_family=xunit2 --junit-xml=junit.xml'
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: 'junit.xml'
                }
            }
        }
    }
}
