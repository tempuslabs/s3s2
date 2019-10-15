pipeline {
    agent {
        docker {
            filename 'Dockerfile.build'
        }
    }
    stages {
        stage('build') {
            steps {
                sh 'make build'
            }
        }
        stage('publish') {
            steps {
                sh 'echo Pretend I published'
            }
        }
    }
}
