=begin
#edna-srv

#No description provided (generated by Swagger Codegen https://github.com/swagger-api/swagger-codegen)

OpenAPI spec version: 0.0.1

Generated by: https://github.com/swagger-api/swagger-codegen.git
Swagger Codegen version: 3.0.30
=end

require 'spec_helper'
require 'json'

# Unit tests for SwaggerClient::DefaultApi
# Automatically generated by swagger-codegen (github.com/swagger-api/swagger-codegen)
# Please update as you see appropriate
describe 'DefaultApi' do
  before do
    # run before each test
    @instance = SwaggerClient::DefaultApi.new
  end

  after do
    # run after each test
  end

  describe 'test an instance of DefaultApi' do
    it 'should create an instance of DefaultApi' do
      expect(@instance).to be_instance_of(SwaggerClient::DefaultApi)
    end
  end

  # unit tests for apiproxy_apply_disguise
  # @param body 
  # @param app 
  # @param did 
  # @param uid 
  # @param [Hash] opts the optional parameters
  # @return [ApplyDisguiseResponse]
  describe 'apiproxy_apply_disguise test' do
    it 'should work' do
      # assertion here. ref: https://www.relishapp.com/rspec/rspec-expectations/docs/built-in-matchers
    end
  end

  # unit tests for apiproxy_cleanup_tokens_of_disguise
  # @param body 
  # @param [Hash] opts the optional parameters
  # @return [nil]
  describe 'apiproxy_cleanup_tokens_of_disguise test' do
    it 'should work' do
      # assertion here. ref: https://www.relishapp.com/rspec/rspec-expectations/docs/built-in-matchers
    end
  end

  # unit tests for apiproxy_create_pseudoprincipal
  # @param [Hash] opts the optional parameters
  # @return [CreatePseudoprincipalResponse]
  describe 'apiproxy_create_pseudoprincipal test' do
    it 'should work' do
      # assertion here. ref: https://www.relishapp.com/rspec/rspec-expectations/docs/built-in-matchers
    end
  end

  # unit tests for apiproxy_end_disguise
  # @param did 
  # @param [Hash] opts the optional parameters
  # @return [EndDisguiseResponse]
  describe 'apiproxy_end_disguise test' do
    it 'should work' do
      # assertion here. ref: https://www.relishapp.com/rspec/rspec-expectations/docs/built-in-matchers
    end
  end

  # unit tests for apiproxy_get_pseudoprincipals_of
  # @param body 
  # @param [Hash] opts the optional parameters
  # @return [Array<String>]
  describe 'apiproxy_get_pseudoprincipals_of test' do
    it 'should work' do
      # assertion here. ref: https://www.relishapp.com/rspec/rspec-expectations/docs/built-in-matchers
    end
  end

  # unit tests for apiproxy_get_tokens_of_disguise
  # @param body 
  # @param [Hash] opts the optional parameters
  # @return [GetTokensOfDisguiseResponse]
  describe 'apiproxy_get_tokens_of_disguise test' do
    it 'should work' do
      # assertion here. ref: https://www.relishapp.com/rspec/rspec-expectations/docs/built-in-matchers
    end
  end

  # unit tests for apiproxy_register_principal
  # @param body 
  # @param [Hash] opts the optional parameters
  # @return [RegisterPrincipalResponse]
  describe 'apiproxy_register_principal test' do
    it 'should work' do
      # assertion here. ref: https://www.relishapp.com/rspec/rspec-expectations/docs/built-in-matchers
    end
  end

  # unit tests for apiproxy_reveal_disguise
  # @param body 
  # @param did 
  # @param [Hash] opts the optional parameters
  # @return [nil]
  describe 'apiproxy_reveal_disguise test' do
    it 'should work' do
      # assertion here. ref: https://www.relishapp.com/rspec/rspec-expectations/docs/built-in-matchers
    end
  end

  # unit tests for apiproxy_save_diff_token
  # @param body 
  # @param [Hash] opts the optional parameters
  # @return [nil]
  describe 'apiproxy_save_diff_token test' do
    it 'should work' do
      # assertion here. ref: https://www.relishapp.com/rspec/rspec-expectations/docs/built-in-matchers
    end
  end

  # unit tests for apiproxy_save_pseudoprincipal_token
  # @param body 
  # @param [Hash] opts the optional parameters
  # @return [nil]
  describe 'apiproxy_save_pseudoprincipal_token test' do
    it 'should work' do
      # assertion here. ref: https://www.relishapp.com/rspec/rspec-expectations/docs/built-in-matchers
    end
  end

  # unit tests for apiproxy_start_disguise
  # @param did 
  # @param [Hash] opts the optional parameters
  # @return [nil]
  describe 'apiproxy_start_disguise test' do
    it 'should work' do
      # assertion here. ref: https://www.relishapp.com/rspec/rspec-expectations/docs/built-in-matchers
    end
  end

  # unit tests for index
  # @param [Hash] opts the optional parameters
  # @return [String]
  describe 'index test' do
    it 'should work' do
      # assertion here. ref: https://www.relishapp.com/rspec/rspec-expectations/docs/built-in-matchers
    end
  end

end
