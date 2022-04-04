=begin
#edna-srv

#No description provided (generated by Swagger Codegen https://github.com/swagger-api/swagger-codegen)

OpenAPI spec version: 0.0.1

Generated by: https://github.com/swagger-api/swagger-codegen.git
Swagger Codegen version: 3.0.33
=end

require 'spec_helper'
require 'json'
require 'date'

# Unit tests for SwaggerClient::GetPseudoprincipals
# Automatically generated by swagger-codegen (github.com/swagger-api/swagger-codegen)
# Please update as you see appropriate
describe 'GetPseudoprincipals' do
  before do
    # run before each test
    @instance = SwaggerClient::GetPseudoprincipals.new
  end

  after do
    # run after each test
  end

  describe 'test an instance of GetPseudoprincipals' do
    it 'should create an instance of GetPseudoprincipals' do
      expect(@instance).to be_instance_of(SwaggerClient::GetPseudoprincipals)
    end
  end
  describe 'test attribute "decrypt_cap"' do
    it 'should work' do
      # assertion here. ref: https://www.relishapp.com/rspec/rspec-expectations/docs/built-in-matchers
    end
  end

  describe 'test attribute "ownership_locators"' do
    it 'should work' do
      # assertion here. ref: https://www.relishapp.com/rspec/rspec-expectations/docs/built-in-matchers
    end
  end

end
