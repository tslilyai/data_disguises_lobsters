# swagger_client

SwaggerClient - the Ruby gem for the edna-srv

No description provided (generated by Swagger Codegen https://github.com/swagger-api/swagger-codegen)

This SDK is automatically generated by the [Swagger Codegen](https://github.com/swagger-api/swagger-codegen) project:

- API version: 0.0.1
- Package version: 1.0.0
- Build package: io.swagger.codegen.v3.generators.ruby.RubyClientCodegen

## Installation

### Build a gem

To build the Ruby code into a gem:

```shell
gem build swagger_client.gemspec
```

Then either install the gem locally:

```shell
gem install ./swagger_client-1.0.0.gem
```
(for development, run `gem install --dev ./swagger_client-1.0.0.gem` to install the development dependencies)

or publish the gem to a gem hosting service, e.g. [RubyGems](https://rubygems.org/).

Finally add this to the Gemfile:

    gem 'swagger_client', '~> 1.0.0'

### Install from Git

If the Ruby gem is hosted at a git repository: https://github.com/GIT_USER_ID/GIT_REPO_ID, then add the following in the Gemfile:

    gem 'swagger_client', :git => 'https://github.com/GIT_USER_ID/GIT_REPO_ID.git'

### Include the Ruby code directly

Include the Ruby code directly using `-I` as follows:

```shell
ruby -Ilib script.rb
```

## Getting Started

Please follow the [installation](#installation) procedure and then run the following code:
```ruby
# Load the gem
require 'swagger_client'

api_instance = SwaggerClient::DefaultApi.new
body = SwaggerClient::ApplyDisguise.new # ApplyDisguise | 


begin
  result = api_instance.apiproxy_apply_disguise(body)
  p result
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_apply_disguise: #{e}"
end

api_instance = SwaggerClient::DefaultApi.new
body = SwaggerClient::CleanupRecordsOfDisguise.new # CleanupRecordsOfDisguise | 


begin
  api_instance.apiproxy_cleanup_records_of_disguise(body)
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_cleanup_records_of_disguise: #{e}"
end

api_instance = SwaggerClient::DefaultApi.new

begin
  result = api_instance.apiproxy_create_pseudoprincipal
  p result
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_create_pseudoprincipal: #{e}"
end

api_instance = SwaggerClient::DefaultApi.new
did = 56 # Integer | 


begin
  result = api_instance.apiproxy_end_disguise(did)
  p result
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_end_disguise: #{e}"
end

api_instance = SwaggerClient::DefaultApi.new
did = 56 # Integer | 


begin
  api_instance.apiproxy_end_reveal(did)
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_end_reveal: #{e}"
end

api_instance = SwaggerClient::DefaultApi.new
body = SwaggerClient::GetPseudoprincipals.new # GetPseudoprincipals | 


begin
  result = api_instance.apiproxy_get_pseudoprincipals_of(body)
  p result
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_get_pseudoprincipals_of: #{e}"
end

api_instance = SwaggerClient::DefaultApi.new
body = SwaggerClient::GetRecordsOfDisguise.new # GetRecordsOfDisguise | 


begin
  result = api_instance.apiproxy_get_records_of_disguise(body)
  p result
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_get_records_of_disguise: #{e}"
end

api_instance = SwaggerClient::DefaultApi.new
body = 'body_example' # String | 


begin
  result = api_instance.apiproxy_register_principal(body)
  p result
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_register_principal: #{e}"
end

api_instance = SwaggerClient::DefaultApi.new
body = SwaggerClient::RevealDisguise.new # RevealDisguise | 
did = 56 # Integer | 


begin
  api_instance.apiproxy_reveal_disguise(body, did)
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_reveal_disguise: #{e}"
end

api_instance = SwaggerClient::DefaultApi.new
body = SwaggerClient::SaveDiffRecord.new # SaveDiffRecord | 


begin
  api_instance.apiproxy_save_diff_record(body)
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_save_diff_record: #{e}"
end

api_instance = SwaggerClient::DefaultApi.new
body = SwaggerClient::SavePseudoprincipalRecord.new # SavePseudoprincipalRecord | 


begin
  api_instance.apiproxy_save_pseudoprincipal_record(body)
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_save_pseudoprincipal_record: #{e}"
end

api_instance = SwaggerClient::DefaultApi.new

begin
  result = api_instance.apiproxy_start_disguise
  p result
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_start_disguise: #{e}"
end

api_instance = SwaggerClient::DefaultApi.new
did = 56 # Integer | 


begin
  api_instance.apiproxy_start_reveal(did)
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_start_reveal: #{e}"
end

api_instance = SwaggerClient::DefaultApi.new

begin
  result = api_instance.index
  p result
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->index: #{e}"
end
```

## Documentation for API Endpoints

All URIs are relative to */*

Class | Method | HTTP request | Description
------------ | ------------- | ------------- | -------------
*SwaggerClient::DefaultApi* | [**apiproxy_apply_disguise**](docs/DefaultApi.md#apiproxy_apply_disguise) | **POST** /apply_disguise | 
*SwaggerClient::DefaultApi* | [**apiproxy_cleanup_records_of_disguise**](docs/DefaultApi.md#apiproxy_cleanup_records_of_disguise) | **POST** /cleanup_records_of_disguise | 
*SwaggerClient::DefaultApi* | [**apiproxy_create_pseudoprincipal**](docs/DefaultApi.md#apiproxy_create_pseudoprincipal) | **GET** /create_pp | 
*SwaggerClient::DefaultApi* | [**apiproxy_end_disguise**](docs/DefaultApi.md#apiproxy_end_disguise) | **GET** /end_disguise/{did} | 
*SwaggerClient::DefaultApi* | [**apiproxy_end_reveal**](docs/DefaultApi.md#apiproxy_end_reveal) | **GET** /end_reveal/{did} | 
*SwaggerClient::DefaultApi* | [**apiproxy_get_pseudoprincipals_of**](docs/DefaultApi.md#apiproxy_get_pseudoprincipals_of) | **POST** /get_pps_of | 
*SwaggerClient::DefaultApi* | [**apiproxy_get_records_of_disguise**](docs/DefaultApi.md#apiproxy_get_records_of_disguise) | **POST** /get_records_of_disguise | 
*SwaggerClient::DefaultApi* | [**apiproxy_register_principal**](docs/DefaultApi.md#apiproxy_register_principal) | **POST** /register_principal | 
*SwaggerClient::DefaultApi* | [**apiproxy_reveal_disguise**](docs/DefaultApi.md#apiproxy_reveal_disguise) | **POST** /reveal_disguise/{did} | 
*SwaggerClient::DefaultApi* | [**apiproxy_save_diff_record**](docs/DefaultApi.md#apiproxy_save_diff_record) | **POST** /save_diff_record | 
*SwaggerClient::DefaultApi* | [**apiproxy_save_pseudoprincipal_record**](docs/DefaultApi.md#apiproxy_save_pseudoprincipal_record) | **POST** /save_pp_record | 
*SwaggerClient::DefaultApi* | [**apiproxy_start_disguise**](docs/DefaultApi.md#apiproxy_start_disguise) | **GET** /start_disguise | 
*SwaggerClient::DefaultApi* | [**apiproxy_start_reveal**](docs/DefaultApi.md#apiproxy_start_reveal) | **GET** /start_reveal/{did} | 
*SwaggerClient::DefaultApi* | [**index**](docs/DefaultApi.md#index) | **GET** / | 

## Documentation for Models

 - [SwaggerClient::APILocCap](docs/APILocCap.md)
 - [SwaggerClient::APIRowVal](docs/APIRowVal.md)
 - [SwaggerClient::ApplyDisguise](docs/ApplyDisguise.md)
 - [SwaggerClient::ApplyDisguiseResponse](docs/ApplyDisguiseResponse.md)
 - [SwaggerClient::CleanupRecordsOfDisguise](docs/CleanupRecordsOfDisguise.md)
 - [SwaggerClient::CreatePseudoprincipalResponse](docs/CreatePseudoprincipalResponse.md)
 - [SwaggerClient::EndDisguiseResponse](docs/EndDisguiseResponse.md)
 - [SwaggerClient::GetPseudoprincipals](docs/GetPseudoprincipals.md)
 - [SwaggerClient::GetRecordsOfDisguise](docs/GetRecordsOfDisguise.md)
 - [SwaggerClient::GetRecordsOfDisguiseResponse](docs/GetRecordsOfDisguiseResponse.md)
 - [SwaggerClient::RegisterPrincipalResponse](docs/RegisterPrincipalResponse.md)
 - [SwaggerClient::RevealDisguise](docs/RevealDisguise.md)
 - [SwaggerClient::SaveDiffRecord](docs/SaveDiffRecord.md)
 - [SwaggerClient::SavePseudoprincipalRecord](docs/SavePseudoprincipalRecord.md)
 - [SwaggerClient::StartDisguiseResponse](docs/StartDisguiseResponse.md)

## Documentation for Authorization

 All endpoints do not require authorization.
