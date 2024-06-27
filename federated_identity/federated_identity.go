package federatedidentity

import (
	"context"
	log "github.com/sirupsen/logrus"
	"os"

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

func FederatedIdentityConfig(ctx context.Context, roleArn *string, region *string, tokenRetriever *FederatedIdentityTokenRetriever) (*session.Session, error) {

	_, inK8s := os.LookupEnv("KUBERNETES_SERVICE_HOST")
	if !inK8s {
		return nil, &NotInKubernetesError{}
	}

	var regionString string
	if region != nil {
		regionString = *region
	} else if envRegion, ok := os.LookupEnv("AWS_REGION"); ok {
		regionString = envRegion
	} else {
		log.Debugf("neither region nor $AWS_REGION defined; defaulting to 'us-east-1'")
		regionString = "us-east-1"
	}

	token, err := tokenRetriever.GetIdentityToken()
	if err != nil {
		log.Printf("Failed to fetch identity token: %v", err)
		return nil, err
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(regionString),
	})
	if err != nil {
		return nil, err
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
		return nil, err
	}

	// Update the session credentials
	sess.Config.Credentials = credentials.NewStaticCredentials(
		*assumeRoleOutput.Credentials.AccessKeyId,
		*assumeRoleOutput.Credentials.SecretAccessKey,
		*assumeRoleOutput.Credentials.SessionToken,
	)
	return sess, nil
	//creds := stscreds.NewWebIdentityCredentials(sess, *roleArn, "golang-federated-identity", "")

	//cfg := aws.NewConfig().WithRegion(regionString).WithCredentials(creds)
	//log.Debugf("Using AWS Config '%s'", cfg)
	//return cfg, nil
}
