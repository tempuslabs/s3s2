package federatedidentity

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"google.golang.org/api/idtoken"
)

var EXPIRY_WINDOW_SECONDS = 60 * 60 // 60 minutes

type FederatedIdentityTokenRetriever struct{}

func (f *FederatedIdentityTokenRetriever) GetIdentityToken() ([]byte, error) {
	tokenSource, err := idtoken.NewTokenSource(context.Background(), "unused")
	if err != nil {
		return []byte{}, err
	}
	token, err := tokenSource.Token()
	if err != nil {
		return []byte{}, err
	}
	return []byte(token.AccessToken), nil
}

type NotInKubernetesError struct{}

func (e *NotInKubernetesError) Error() string {
	return "not in Kubernetes"
}

type NoRoleArnError struct{}

func (e *NoRoleArnError) Error() string {
	return "neither role nor $AWS_ROLE_ARN provided"
}

func FederatedIdentityConfig(ctx context.Context, roleArn *string, region *string) (*aws.Config, error) {
	_, inK8s := os.LookupEnv("KUBERNETES_SERVICE_HOST")
	if !inK8s {
		return nil, &NotInKubernetesError{}
	}

	if roleArn == nil {
		envArn, envArnExists := os.LookupEnv("AWS_ROLE_ARN")
		if !envArnExists {
			return nil, &NoRoleArnError{}
		} else {
			roleArn = &envArn
		}
	}

	var regionString string
	if region != nil {
		regionString = *region
	} else if envRegion, ok := os.LookupEnv("AWS_REGION"); ok {
		regionString = envRegion
	} else {
		log.Default().Println("neither region nor $AWS_REGION defined; defaulting to 'us-east-1'")
		regionString = "us-east-1"
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(regionString),
	})
	if err != nil {
		return nil, err
	}

	creds := stscreds.NewWebIdentityCredentials(sess, *roleArn, "golang-federated-identity", "")

	cfg := aws.NewConfig().WithRegion(regionString).WithCredentials(creds)
	return cfg, nil
}
