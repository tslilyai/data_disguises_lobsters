=begin
#edna-srv

#No description provided (generated by Swagger Codegen https://github.com/swagger-api/swagger-codegen)

OpenAPI spec version: 0.0.1

Generated by: https://github.com/swagger-api/swagger-codegen.git
Swagger Codegen version: 3.0.33
=end

# Common files
require 'swagger_client/api_client'
require 'swagger_client/api_error'
require 'swagger_client/version'
require 'swagger_client/configuration'

# Models
require 'swagger_client/models/api_locator'
require 'swagger_client/models/apply_disguise'
require 'swagger_client/models/apply_disguise_response'
require 'swagger_client/models/cleanup_records_of_disguise'
require 'swagger_client/models/end_disguise_response'
require 'swagger_client/models/get_pseudoprincipals'
require 'swagger_client/models/get_records_of_disguise'
require 'swagger_client/models/get_records_of_disguise_response'
require 'swagger_client/models/register_principal_response'
require 'swagger_client/models/reveal_disguise'
require 'swagger_client/models/save_diff_record'
require 'swagger_client/models/save_pseudoprincipal_record'
require 'swagger_client/models/start_disguise_response'

# APIs
require 'swagger_client/api/default_api'

module SwaggerClient
  class << self
    # Customize default settings for the SDK using block.
    #   SwaggerClient.configure do |config|
    #     config.username = "xxx"
    #     config.password = "xxx"
    #   end
    # If no block given, return the default Configuration object.
    def configure
      if block_given?
        yield(Configuration.default)
      else
        Configuration.default
      end
    end
  end
end
