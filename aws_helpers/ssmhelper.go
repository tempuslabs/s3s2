package aws_helpers

import (
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/aws/aws-sdk-go/service/ssm"
	options "github.com/tempuslabs/s3s2/options"
	utils "github.com/tempuslabs/s3s2/utils"
)

// Fetches value associated with provided keyname from SSM store
func GetParameterValue(ssm_service *ssm.SSM, keyname string, opts options.Options) string {
    withDecryption := true
	var param_value string 

	err := retry.Do(
		func() error {
			param, err := ssm_service.GetParameter(&ssm.GetParameterInput{
				Name:           &keyname,
				WithDecryption: &withDecryption,
			})
			if err != nil { // Wait 10 seconds and crteate a new session
				time.Sleep(10 * time.Second)
				sess := utils.GetAwsSession(opts)
				ssm_service = ssm.New(sess)
				return err
			}
			
			param_value = *param.Parameter.Value
			return nil
		},
			retry.Attempts(3),
	)

	utils.PanicIfError("Error getting SSM parameter - ", err)

    return param_value
}
