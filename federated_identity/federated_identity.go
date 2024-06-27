package federatedidentity

import (
	"context"
	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/aws/aws-sdk-go/aws/session"
	"google.golang.org/api/idtoken"
)

var EXPIRY_WINDOW_SECONDS = 60 * 60 // 60 minutes

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
	// TODO: Remove after testing
	log.Debugf("Assumed role output: %s", *assumeRoleOutput)

	// Update the session credentials
	sess.Config.Credentials = credentials.NewStaticCredentials(
		*assumeRoleOutput.Credentials.AccessKeyId,
		*assumeRoleOutput.Credentials.SecretAccessKey,
		*assumeRoleOutput.Credentials.SessionToken,
	)

	return nil
}
