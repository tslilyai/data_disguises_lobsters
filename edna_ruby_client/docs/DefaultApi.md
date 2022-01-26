# SwaggerClient::DefaultApi

All URIs are relative to */*

Method | HTTP request | Description
------------- | ------------- | -------------
[**apiproxy_apply_disguise**](DefaultApi.md#apiproxy_apply_disguise) | **POST** /apply_disguise | 
[**apiproxy_cleanup_records_of_disguise**](DefaultApi.md#apiproxy_cleanup_records_of_disguise) | **POST** /cleanup_records_of_disguise | 
[**apiproxy_create_pseudoprincipal**](DefaultApi.md#apiproxy_create_pseudoprincipal) | **GET** /create_pp | 
[**apiproxy_end_disguise**](DefaultApi.md#apiproxy_end_disguise) | **GET** /end_disguise/{did} | 
[**apiproxy_end_reveal**](DefaultApi.md#apiproxy_end_reveal) | **GET** /end_reveal/{did} | 
[**apiproxy_get_pseudoprincipals_of**](DefaultApi.md#apiproxy_get_pseudoprincipals_of) | **POST** /get_pps_of | 
[**apiproxy_get_records_of_disguise**](DefaultApi.md#apiproxy_get_records_of_disguise) | **POST** /get_records_of_disguise | 
[**apiproxy_register_principal**](DefaultApi.md#apiproxy_register_principal) | **POST** /register_principal | 
[**apiproxy_reveal_disguise**](DefaultApi.md#apiproxy_reveal_disguise) | **POST** /reveal_disguise/{did} | 
[**apiproxy_save_diff_record**](DefaultApi.md#apiproxy_save_diff_record) | **POST** /save_diff_record | 
[**apiproxy_save_pseudoprincipal_record**](DefaultApi.md#apiproxy_save_pseudoprincipal_record) | **POST** /save_pp_record | 
[**apiproxy_start_disguise**](DefaultApi.md#apiproxy_start_disguise) | **GET** /start_disguise | 
[**apiproxy_start_reveal**](DefaultApi.md#apiproxy_start_reveal) | **GET** /start_reveal/{did} | 
[**index**](DefaultApi.md#index) | **GET** / | 

# **apiproxy_apply_disguise**
> ApplyDisguiseResponse apiproxy_apply_disguise(body)



### Example
```ruby
# load the gem
require 'swagger_client'

api_instance = SwaggerClient::DefaultApi.new
body = SwaggerClient::ApplyDisguise.new # ApplyDisguise | 


begin
  result = api_instance.apiproxy_apply_disguise(body)
  p result
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_apply_disguise: #{e}"
end
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | [**ApplyDisguise**](ApplyDisguise.md)|  | 

### Return type

[**ApplyDisguiseResponse**](ApplyDisguiseResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json



# **apiproxy_cleanup_records_of_disguise**
> apiproxy_cleanup_records_of_disguise(body)



### Example
```ruby
# load the gem
require 'swagger_client'

api_instance = SwaggerClient::DefaultApi.new
body = SwaggerClient::CleanupRecordsOfDisguise.new # CleanupRecordsOfDisguise | 


begin
  api_instance.apiproxy_cleanup_records_of_disguise(body)
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_cleanup_records_of_disguise: #{e}"
end
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | [**CleanupRecordsOfDisguise**](CleanupRecordsOfDisguise.md)|  | 

### Return type

nil (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined



# **apiproxy_create_pseudoprincipal**
> CreatePseudoprincipalResponse apiproxy_create_pseudoprincipal



### Example
```ruby
# load the gem
require 'swagger_client'

api_instance = SwaggerClient::DefaultApi.new

begin
  result = api_instance.apiproxy_create_pseudoprincipal
  p result
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_create_pseudoprincipal: #{e}"
end
```

### Parameters
This endpoint does not need any parameter.

### Return type

[**CreatePseudoprincipalResponse**](CreatePseudoprincipalResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json



# **apiproxy_end_disguise**
> EndDisguiseResponse apiproxy_end_disguise(did)



### Example
```ruby
# load the gem
require 'swagger_client'

api_instance = SwaggerClient::DefaultApi.new
did = 56 # Integer | 


begin
  result = api_instance.apiproxy_end_disguise(did)
  p result
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_end_disguise: #{e}"
end
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **did** | **Integer**|  | 

### Return type

[**EndDisguiseResponse**](EndDisguiseResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json



# **apiproxy_end_reveal**
> apiproxy_end_reveal(did)



### Example
```ruby
# load the gem
require 'swagger_client'

api_instance = SwaggerClient::DefaultApi.new
did = 56 # Integer | 


begin
  api_instance.apiproxy_end_reveal(did)
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_end_reveal: #{e}"
end
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **did** | **Integer**|  | 

### Return type

nil (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined



# **apiproxy_get_pseudoprincipals_of**
> Array&lt;String&gt; apiproxy_get_pseudoprincipals_of(body)



### Example
```ruby
# load the gem
require 'swagger_client'

api_instance = SwaggerClient::DefaultApi.new
body = SwaggerClient::GetPseudoprincipals.new # GetPseudoprincipals | 


begin
  result = api_instance.apiproxy_get_pseudoprincipals_of(body)
  p result
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_get_pseudoprincipals_of: #{e}"
end
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | [**GetPseudoprincipals**](GetPseudoprincipals.md)|  | 

### Return type

**Array&lt;String&gt;**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json



# **apiproxy_get_records_of_disguise**
> GetRecordsOfDisguiseResponse apiproxy_get_records_of_disguise(body)



### Example
```ruby
# load the gem
require 'swagger_client'

api_instance = SwaggerClient::DefaultApi.new
body = SwaggerClient::GetRecordsOfDisguise.new # GetRecordsOfDisguise | 


begin
  result = api_instance.apiproxy_get_records_of_disguise(body)
  p result
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_get_records_of_disguise: #{e}"
end
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | [**GetRecordsOfDisguise**](GetRecordsOfDisguise.md)|  | 

### Return type

[**GetRecordsOfDisguiseResponse**](GetRecordsOfDisguiseResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json



# **apiproxy_register_principal**
> RegisterPrincipalResponse apiproxy_register_principal(body)



### Example
```ruby
# load the gem
require 'swagger_client'

api_instance = SwaggerClient::DefaultApi.new
body = 'body_example' # String | 


begin
  result = api_instance.apiproxy_register_principal(body)
  p result
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_register_principal: #{e}"
end
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | [**String**](String.md)|  | 

### Return type

[**RegisterPrincipalResponse**](RegisterPrincipalResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/octet-stream
 - **Accept**: application/json



# **apiproxy_reveal_disguise**
> apiproxy_reveal_disguise(bodydid)



### Example
```ruby
# load the gem
require 'swagger_client'

api_instance = SwaggerClient::DefaultApi.new
body = SwaggerClient::RevealDisguise.new # RevealDisguise | 
did = 56 # Integer | 


begin
  api_instance.apiproxy_reveal_disguise(bodydid)
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_reveal_disguise: #{e}"
end
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | [**RevealDisguise**](RevealDisguise.md)|  | 
 **did** | **Integer**|  | 

### Return type

nil (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined



# **apiproxy_save_diff_record**
> apiproxy_save_diff_record(body)



### Example
```ruby
# load the gem
require 'swagger_client'

api_instance = SwaggerClient::DefaultApi.new
body = SwaggerClient::SaveDiffRecord.new # SaveDiffRecord | 


begin
  api_instance.apiproxy_save_diff_record(body)
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_save_diff_record: #{e}"
end
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | [**SaveDiffRecord**](SaveDiffRecord.md)|  | 

### Return type

nil (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined



# **apiproxy_save_pseudoprincipal_record**
> apiproxy_save_pseudoprincipal_record(body)



### Example
```ruby
# load the gem
require 'swagger_client'

api_instance = SwaggerClient::DefaultApi.new
body = SwaggerClient::SavePseudoprincipalRecord.new # SavePseudoprincipalRecord | 


begin
  api_instance.apiproxy_save_pseudoprincipal_record(body)
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_save_pseudoprincipal_record: #{e}"
end
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | [**SavePseudoprincipalRecord**](SavePseudoprincipalRecord.md)|  | 

### Return type

nil (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined



# **apiproxy_start_disguise**
> StartDisguiseResponse apiproxy_start_disguise



### Example
```ruby
# load the gem
require 'swagger_client'

api_instance = SwaggerClient::DefaultApi.new

begin
  result = api_instance.apiproxy_start_disguise
  p result
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_start_disguise: #{e}"
end
```

### Parameters
This endpoint does not need any parameter.

### Return type

[**StartDisguiseResponse**](StartDisguiseResponse.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json



# **apiproxy_start_reveal**
> apiproxy_start_reveal(did)



### Example
```ruby
# load the gem
require 'swagger_client'

api_instance = SwaggerClient::DefaultApi.new
did = 56 # Integer | 


begin
  api_instance.apiproxy_start_reveal(did)
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->apiproxy_start_reveal: #{e}"
end
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **did** | **Integer**|  | 

### Return type

nil (empty response body)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined



# **index**
> String index



### Example
```ruby
# load the gem
require 'swagger_client'

api_instance = SwaggerClient::DefaultApi.new

begin
  result = api_instance.index
  p result
rescue SwaggerClient::ApiError => e
  puts "Exception when calling DefaultApi->index: #{e}"
end
```

### Parameters
This endpoint does not need any parameter.

### Return type

**String**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: text/plain


