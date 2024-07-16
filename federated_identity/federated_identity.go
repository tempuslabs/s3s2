package federatedidentity

import (
	"context"
	"time"
	"sync"
	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/aws/aws-sdk-go/aws/session"
	"google.golang.org/api/idtoken"
)

var EXPIRY_WINDOW_SECONDS = 60 * 60 // 60 minutes

var (
	mu        sync.Mutex
	stopCh    chan struct{}
	running   bool
)

type FederatedIdentityTokenRetriever struct{}

func (f *FederatedIdentityTokenRetriever) GetIdentityToken() (string, error) {
	tokenSource, err := idtoken.NewTokenSource(context.Background(), "unused")
	if err != nil {
		return "", err
	}
	token, err := tokenSource.Token()
	if err != nil {
		return "", err
	}
	return token.AccessToken, nil
}

type NotInKubernetesError struct{}

func (e *NotInKubernetesError) Error() string {
	return "not in Kubernetes"
}

type NoRoleArnError struct{}

func (e *NoRoleArnError) Error() string {
	return "neither role nor $AWS_ROLE_ARN provided"
}

func FederatedIdentityConfig(sess *session.Session, roleArn *string, tokenRetriever *FederatedIdentityTokenRetriever) (error) {

	token, err := tokenRetriever.GetIdentityToken()
	if err != nil {
		log.Debugf("Failed to fetch identity token: %v", err)
		return err
	}

	// Create STS client
	stsSvc := sts.New(sess)

	// Assume the role using the token
	assumeRoleOutput, err := stsSvc.AssumeRoleWithWebIdentity(&sts.AssumeRoleWithWebIdentityInput{
		RoleArn:          aws.String(*roleArn),
		RoleSessionName:  aws.String("golang-federated-identity"),
		WebIdentityToken: aws.String(token),
		DurationSeconds:  aws.Int64(int64(EXPIRY_WINDOW_SECONDS)),
	})
	
	if err != nil {
		log.Printf("Failed to assume role: %v", err)
		return err
	}

	// Update the session credentials
	sess.Config.Credentials = credentials.NewStaticCredentials(
		*assumeRoleOutput.Credentials.AccessKeyId,
		*assumeRoleOutput.Credentials.SecretAccessKey,
		*assumeRoleOutput.Credentials.SessionToken,
	)

	return nil
}

func StartRefreshSession(sess *session.Session, roleArn *string) {
	// Check that the session is not already running
	// If it is, return otherwise set it to running and start
	// the goroutine with RefreshSession
	mu.Lock()
	defer mu.Unlock()
	if running {
		log.Debugf("RefreshSession already running...")
		return
	}
	stopCh = make(chan struct{})
	running = true
	go RefreshSession(sess, roleArn)
}

func StopRefreshSession() {
	// Check that the session is running and if it is, close 
	// the stop channel
	mu.Lock()
	defer mu.Unlock()
	if running {
		log.Debugf("Stopping RefreshSession...")
		close(stopCh)
		running = false
	}
}

func RefreshSession(sess *session.Session, roleArn *string) {
    for {
		log.Debugf("Refreshing credentials")
		// Refresh the credentials every before EXPIRY_WINDOW_SECONDS
		refreshSeconds := EXPIRY_WINDOW_SECONDS - 120
		if refreshSeconds < 0 {
			// If the EXPRIY_WINDOW_SECONDS is less than 120 seconds,
			// refresh every second
			refreshSeconds = 1
		}
		time.Sleep(time.Duration(refreshSeconds) * time.Second)
        tokenRetriever := FederatedIdentityTokenRetriever{}
        FederatedIdentityConfig(sess, roleArn, &tokenRetriever)
    }
}
