use decor::policy::{GeneratePolicy, GuiseColumnPolicy, EntityGuisePolicies, KeyRelationship, ApplicationPolicy};
use std::collections::HashMap;

fn get_guise_policies() -> EntityGuisePolicies {
    let mut guise_policies : EntityGuisePolicies = HashMap::new();

    let mut customer_map = HashMap::new();
    customer_map.insert("id".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Random));
    customers_map.insert("username".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Random));
    customers_map.insert("karma".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    guise_policies.insert("customers".to_string(), users_map);

    let mut stories_map = HashMap::new();
    stories_map.insert("id".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Random));
    //stories_map.insert("created_at".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Custom(Box::new(|time| time)))); 
    ////TODO custom functions not supported because of clone / hash reasons... 
    stories_map.insert("created_at".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Default(0.to_string()))); //TODO randomize
    stories_map.insert("user_id".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::ForeignKey("users".to_string())));
    stories_map.insert("url".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("title".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("description".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("short_id".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Random));
    stories_map.insert("is_expired".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Random));
    stories_map.insert("upvotes".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("downvotes".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("is_moderated".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("hotness".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("markeddown_description".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("story_cache".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("comments_count".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    stories_map.insert("merged_story_id".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Default("NULL".to_string())));
    stories_map.insert("unavailable_at".to_string(), GuiseColumnPolicy::CloneAll);
    stories_map.insert("twitter_id".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Default("NULL".to_string())));
    stories_map.insert("user_is_author".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Default(0.to_string())));
    guise_policies.insert("stories".to_string(), stories_map);

    let mut taggings_map = HashMap::new();
    taggings_map.insert("id".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::Random));
    taggings_map.insert("story_id".to_string(), GuiseColumnPolicy::Generate(GeneratePolicy::ForeignKey("stories".to_string())));
    taggings_map.insert("tag_id".to_string(), GuiseColumnPolicy::CloneAll);
    guise_policies.insert("taggings".to_string(), taggings_map);
    
    guise_policies 
}

fn get_prestashop_policy() -> ApplicationPolicy {
    use decor::policy::DecorrelationPolicy::*;
    ApplicationPolicy{
        entity_type_to_decorrelate: "users".to_string(), 
        guise_policies : get_guise_policies(), 
        edge_policies : vec![
            KeyRelationship{
                child: "accessory".to_string(),
                parent: "product_1".to_string(),
                column_name: "id_product_1".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "accessory".to_string(),
                parent: "product_2".to_string(),
                column_name: "id_product_2".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "address".to_string(),
                parent: "customer".to_string(),
                column_name: "id_customer".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "address".to_string(),
                parent: "country".to_string(),
                column_name: "id_country".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "address".to_string(),
                parent: "state".to_string(),
                column_name: "id_state".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "address".to_string(),
                parent: "manufacturer".to_string(),
                column_name: "id_manufacturer".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "address".to_string(),
                parent: "supplier".to_string(),
                column_name: "id_supplier".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "address".to_string(),
                parent: "warehouse".to_string(),
                column_name: "id_warehouse".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "attachment_lang".to_string(),
                parent: "attachment".to_string(),
                column_name: "id_attachment".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "attachment_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_attachment".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_attachment".to_string(),
                parent: "attachment".to_string(),
                column_name: "id_attachment".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "attribute_impact".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "attribute_impact".to_string(),
                parent: "attribute".to_string(),
                column_name: "id_attribute".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "carrier".to_string(),
                parent: "tax_rules_group".to_string(),
                column_name: "id_tax_rules_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "carrier_zone".to_string(),
                parent: "carrier".to_string(),
                column_name: "id_carrier".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "carrier_zone".to_string(),
                parent: "zone".to_string(),
                column_name: "id_zone".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart".to_string(),
                parent: "customer".to_string(),
                column_name: "id_customer".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart".to_string(),
                parent: "address_delivery".to_string(),
                column_name: "id_address_delivery".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart".to_string(),
                parent: "address_invoice".to_string(),
                column_name: "id_address_invoice".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart".to_string(),
                parent: "carrier".to_string(),
                column_name: "id_carrier".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart".to_string(),
                parent: "currency".to_string(),
                column_name: "id_currency".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart".to_string(),
                parent: "guest".to_string(),
                column_name: "id_guest".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart".to_string(),
                parent: "shop_group".to_string(),
                column_name: "id_shop_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_rule_lang".to_string(),
                parent: "cart_rule".to_string(),
                column_name: "id_cart_rule".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_rule_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_rule_country".to_string(),
                parent: "cart_rule".to_string(),
                column_name: "id_cart_rule".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_rule_country".to_string(),
                parent: "country".to_string(),
                column_name: "id_country".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_rule_group".to_string(),
                parent: "cart_rule".to_string(),
                column_name: "id_cart_rule".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_rule_group".to_string(),
                parent: "group".to_string(),
                column_name: "id_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_rule_carrier".to_string(),
                parent: "cart_rule".to_string(),
                column_name: "id_cart_rule".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_rule_carrier".to_string(),
                parent: "carrier".to_string(),
                column_name: "id_carrier".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_rule_combination".to_string(),
                parent: "cart_rule_1".to_string(),
                column_name: "id_cart_rule_1".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_rule_combination".to_string(),
                parent: "cart_rule_2".to_string(),
                column_name: "id_cart_rule_2".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_rule_product_rule_group".to_string(),
                parent: "product_rule_group".to_string(),
                column_name: "id_product_rule_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_rule_product_rule".to_string(),
                parent: "product_rule".to_string(),
                column_name: "id_product_rule".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_rule_product_rule_value".to_string(),
                parent: "product_rule".to_string(),
                column_name: "id_product_rule".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_rule_product_rule_value".to_string(),
                parent: "item".to_string(),
                column_name: "id_item".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_cart_rule".to_string(),
                parent: "cart".to_string(),
                column_name: "id_cart".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_cart_rule".to_string(),
                parent: "cart_rule".to_string(),
                column_name: "id_cart_rule".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_cart_rule".to_string(),
                parent: "cart_rule".to_string(),
                column_name: "id_cart_rule".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_rule_shop".to_string(),
                parent: "cart_rule".to_string(),
                column_name: "id_cart_rule".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_rule_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cart_product".to_string(),
                parent: "product_attribute".to_string(),
                column_name: "id_product_attribute".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "category".to_string(),
                parent: "parent".to_string(),
                column_name: "id_parent".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "category_group".to_string(),
                parent: "category".to_string(),
                column_name: "id_category".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "category_group".to_string(),
                parent: "group".to_string(),
                column_name: "id_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "category_group".to_string(),
                parent: "category".to_string(),
                column_name: "id_category".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "category_group".to_string(),
                parent: "group".to_string(),
                column_name: "id_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "category_product".to_string(),
                parent: "category".to_string(),
                column_name: "id_category".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "category_product".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cms_lang".to_string(),
                parent: "cms".to_string(),
                column_name: "id_cms".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cms_lang".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cms_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cms_category".to_string(),
                parent: "parent".to_string(),
                column_name: "id_parent".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cms_category_shop".to_string(),
                parent: "cms_category".to_string(),
                column_name: "id_cms_category".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cms_category_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cms_category_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "configuration".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "configuration".to_string(),
                parent: "shop_group".to_string(),
                column_name: "id_shop_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "configuration_lang".to_string(),
                parent: "configuration".to_string(),
                column_name: "id_configuration".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "configuration_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "configuration_kpi".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "configuration_kpi".to_string(),
                parent: "shop_group".to_string(),
                column_name: "id_shop_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "connections".to_string(),
                parent: "guest".to_string(),
                column_name: "id_guest".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "connections".to_string(),
                parent: "page".to_string(),
                column_name: "id_page".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "connections_source".to_string(),
                parent: "connections".to_string(),
                column_name: "id_connections".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "contact_lang".to_string(),
                parent: "contact".to_string(),
                column_name: "id_contact".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "contact_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "country".to_string(),
                parent: "zone".to_string(),
                column_name: "id_zone".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "country_lang".to_string(),
                parent: "country".to_string(),
                column_name: "id_country".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "country_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "currency_lang".to_string(),
                parent: "currency".to_string(),
                column_name: "id_currency".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "currency_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "customer".to_string(),
                parent: "gender".to_string(),
                column_name: "id_gender".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "customer".to_string(),
                parent: "shop_group".to_string(),
                column_name: "id_shop_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "customer".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "customer_group".to_string(),
                parent: "customer".to_string(),
                column_name: "id_customer".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "customer_group".to_string(),
                parent: "group".to_string(),
                column_name: "id_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "customer_group".to_string(),
                parent: "customer".to_string(),
                column_name: "id_customer".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "customer_message".to_string(),
                parent: "customer_thread".to_string(),
                column_name: "id_customer_thread".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "customer_message".to_string(),
                parent: "employee".to_string(),
                column_name: "id_employee".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "customer_thread".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "customer_thread".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "customer_thread".to_string(),
                parent: "contact".to_string(),
                column_name: "id_contact".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "customer_thread".to_string(),
                parent: "customer".to_string(),
                column_name: "id_customer".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "customer_thread".to_string(),
                parent: "order".to_string(),
                column_name: "id_order".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "customer_thread".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "customization".to_string(),
                parent: "product_attribute".to_string(),
                column_name: "id_product_attribute".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "customization_field".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "delivery".to_string(),
                parent: "zone".to_string(),
                column_name: "id_zone".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "delivery".to_string(),
                parent: "carrier".to_string(),
                column_name: "id_carrier".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "delivery".to_string(),
                parent: "zone".to_string(),
                column_name: "id_zone".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "delivery".to_string(),
                parent: "range_price".to_string(),
                column_name: "id_range_price".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "delivery".to_string(),
                parent: "range_weight".to_string(),
                column_name: "id_range_weight".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "employee".to_string(),
                parent: "profile".to_string(),
                column_name: "id_profile".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "employee_shop".to_string(),
                parent: "employee".to_string(),
                column_name: "id_employee".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "employee_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "employee_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "feature_lang".to_string(),
                parent: "feature".to_string(),
                column_name: "id_feature".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "feature_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "feature_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "feature_product".to_string(),
                parent: "feature_value".to_string(),
                column_name: "id_feature_value".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "feature_product".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "feature_value".to_string(),
                parent: "feature".to_string(),
                column_name: "id_feature".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "feature_value_lang".to_string(),
                parent: "feature_value".to_string(),
                column_name: "id_feature_value".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "feature_value_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "gender_lang".to_string(),
                parent: "gender".to_string(),
                column_name: "id_gender".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "gender_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "gender_lang".to_string(),
                parent: "gender".to_string(),
                column_name: "id_gender".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "group_lang".to_string(),
                parent: "group".to_string(),
                column_name: "id_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "group_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "group_reduction".to_string(),
                parent: "group".to_string(),
                column_name: "id_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "group_reduction".to_string(),
                parent: "category".to_string(),
                column_name: "id_category".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_group_reduction_cache".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_group_reduction_cache".to_string(),
                parent: "group".to_string(),
                column_name: "id_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "guest".to_string(),
                parent: "customer".to_string(),
                column_name: "id_customer".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "guest".to_string(),
                parent: "operating_system".to_string(),
                column_name: "id_operating_system".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "guest".to_string(),
                parent: "web_browser".to_string(),
                column_name: "id_web_browser".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "hook_module".to_string(),
                parent: "hook".to_string(),
                column_name: "id_hook".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "hook_module".to_string(),
                parent: "module".to_string(),
                column_name: "id_module".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "hook_module".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "hook_module_exceptions".to_string(),
                parent: "module".to_string(),
                column_name: "id_module".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "hook_module_exceptions".to_string(),
                parent: "hook".to_string(),
                column_name: "id_hook".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "image".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "image".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "image_lang".to_string(),
                parent: "image".to_string(),
                column_name: "id_image".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "image_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "image_lang".to_string(),
                parent: "image".to_string(),
                column_name: "id_image".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "manufacturer_lang".to_string(),
                parent: "manufacturer".to_string(),
                column_name: "id_manufacturer".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "manufacturer_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "message".to_string(),
                parent: "order".to_string(),
                column_name: "id_order".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "message".to_string(),
                parent: "cart".to_string(),
                column_name: "id_cart".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "message".to_string(),
                parent: "customer".to_string(),
                column_name: "id_customer".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "message".to_string(),
                parent: "employee".to_string(),
                column_name: "id_employee".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "message_readed".to_string(),
                parent: "message".to_string(),
                column_name: "id_message".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "message_readed".to_string(),
                parent: "employee".to_string(),
                column_name: "id_employee".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "meta_lang".to_string(),
                parent: "meta".to_string(),
                column_name: "id_meta".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "meta_lang".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "meta_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "meta_lang".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "meta_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "module_currency".to_string(),
                parent: "module".to_string(),
                column_name: "id_module".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "orders".to_string(),
                parent: "order".to_string(),
                column_name: "id_order".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "orders".to_string(),
                parent: "customer".to_string(),
                column_name: "id_customer".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "orders".to_string(),
                parent: "cart".to_string(),
                column_name: "id_cart".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "orders".to_string(),
                parent: "carrier".to_string(),
                column_name: "id_carrier".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "orders".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "orders".to_string(),
                parent: "currency".to_string(),
                column_name: "id_currency".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "orders".to_string(),
                parent: "address_delivery".to_string(),
                column_name: "id_address_delivery".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "orders".to_string(),
                parent: "address_invoice".to_string(),
                column_name: "id_address_invoice".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "orders".to_string(),
                parent: "shop_group".to_string(),
                column_name: "id_shop_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "orders".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_detail_tax".to_string(),
                parent: "order_detail".to_string(),
                column_name: "id_order_detail".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_detail_tax".to_string(),
                parent: "tax".to_string(),
                column_name: "id_tax".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_invoice".to_string(),
                parent: "order".to_string(),
                column_name: "id_order".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_invoice_tax".to_string(),
                parent: "tax".to_string(),
                column_name: "id_tax".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_detail".to_string(),
                parent: "order".to_string(),
                column_name: "id_order".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_detail".to_string(),
                parent: "attribute_id".to_string(),
                column_name: "product_attribute_id".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_detail".to_string(),
                parent: "tax_rules_group".to_string(),
                column_name: "id_tax_rules_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_detail".to_string(),
                parent: "order".to_string(),
                column_name: "id_order".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_cart_rule".to_string(),
                parent: "order".to_string(),
                column_name: "id_order".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_cart_rule".to_string(),
                parent: "cart_rule".to_string(),
                column_name: "id_cart_rule".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_history".to_string(),
                parent: "order".to_string(),
                column_name: "id_order".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_history".to_string(),
                parent: "employee".to_string(),
                column_name: "id_employee".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_history".to_string(),
                parent: "order_state".to_string(),
                column_name: "id_order_state".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_message_lang".to_string(),
                parent: "order_message".to_string(),
                column_name: "id_order_message".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_message_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_return".to_string(),
                parent: "customer".to_string(),
                column_name: "id_customer".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_return".to_string(),
                parent: "order".to_string(),
                column_name: "id_order".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_slip".to_string(),
                parent: "customer".to_string(),
                column_name: "id_customer".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_slip".to_string(),
                parent: "order".to_string(),
                column_name: "id_order".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_state_lang".to_string(),
                parent: "order_state".to_string(),
                column_name: "id_order_state".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_state_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "page".to_string(),
                parent: "page_type".to_string(),
                column_name: "id_page_type".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "page".to_string(),
                parent: "object".to_string(),
                column_name: "id_object".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product".to_string(),
                parent: "supplier".to_string(),
                column_name: "id_supplier".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product".to_string(),
                parent: "manufacturer".to_string(),
                column_name: "id_manufacturer".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product".to_string(),
                parent: "category_default".to_string(),
                column_name: "id_category_default".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_shop".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_shop".to_string(),
                parent: "category_default".to_string(),
                column_name: "id_category_default".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_attribute".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_attribute".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_attribute_combination".to_string(),
                parent: "product_attribute".to_string(),
                column_name: "id_product_attribute".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_attribute_image".to_string(),
                parent: "image".to_string(),
                column_name: "id_image".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_download".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_download".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_sale".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_tag".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_tag".to_string(),
                parent: "tag".to_string(),
                column_name: "id_tag".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_tag".to_string(),
                parent: "tag".to_string(),
                column_name: "id_tag".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_tag".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_tag".to_string(),
                parent: "tag".to_string(),
                column_name: "id_tag".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "profile_lang".to_string(),
                parent: "profile".to_string(),
                column_name: "id_profile".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "profile_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "quick_access_lang".to_string(),
                parent: "quick_access".to_string(),
                column_name: "id_quick_access".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "quick_access_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "referrer_shop".to_string(),
                parent: "referrer".to_string(),
                column_name: "id_referrer".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "referrer_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "search_index".to_string(),
                parent: "word".to_string(),
                column_name: "id_word".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "search_index".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "search_index".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "search_word".to_string(),
                parent: "word".to_string(),
                column_name: "id_word".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "search_word".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "search_word".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "specific_price".to_string(),
                parent: "specific_price_rule".to_string(),
                column_name: "id_specific_price_rule".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "specific_price".to_string(),
                parent: "cart".to_string(),
                column_name: "id_cart".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "specific_price".to_string(),
                parent: "product_attribute".to_string(),
                column_name: "id_product_attribute".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "specific_price".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "specific_price".to_string(),
                parent: "customer".to_string(),
                column_name: "id_customer".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "state".to_string(),
                parent: "country".to_string(),
                column_name: "id_country".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "state".to_string(),
                parent: "zone".to_string(),
                column_name: "id_zone".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "supplier_lang".to_string(),
                parent: "supplier".to_string(),
                column_name: "id_supplier".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "supplier_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "tag".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "tag_count".to_string(),
                parent: "group".to_string(),
                column_name: "id_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "tag_count".to_string(),
                parent: "tag".to_string(),
                column_name: "id_tag".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "tax_lang".to_string(),
                parent: "tax".to_string(),
                column_name: "id_tax".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "tax_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "carrier_group".to_string(),
                parent: "carrier".to_string(),
                column_name: "id_carrier".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "carrier_group".to_string(),
                parent: "group".to_string(),
                column_name: "id_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "store_lang".to_string(),
                parent: "store".to_string(),
                column_name: "id_store".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "store_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "webservice_permission".to_string(),
                parent: "webservice_account".to_string(),
                column_name: "id_webservice_account".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_country_tax".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_country_tax".to_string(),
                parent: "country".to_string(),
                column_name: "id_country".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "tax_rule".to_string(),
                parent: "tax_rules_group".to_string(),
                column_name: "id_tax_rules_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "tax_rule".to_string(),
                parent: "tax".to_string(),
                column_name: "id_tax".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "tax_rules_group".to_string(),
                parent: "tax_rules_group` INT NOT NULL AUTO_INCREMENT PRIMARY KEY".to_string(),
                column_name: "id_tax_rules_group` INT NOT NULL AUTO_INCREMENT PRIMARY KEY".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "specific_price_priority".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "shop_url".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "country_shop".to_string(),
                parent: "country".to_string(),
                column_name: "id_country".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "country_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "country_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "carrier_shop".to_string(),
                parent: "carrier".to_string(),
                column_name: "id_carrier".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "carrier_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "carrier_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "address_format".to_string(),
                parent: "country".to_string(),
                column_name: "id_country".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cms_shop".to_string(),
                parent: "cms".to_string(),
                column_name: "id_cms".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cms_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cms_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "currency_shop".to_string(),
                parent: "currency".to_string(),
                column_name: "id_currency".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "currency_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "currency_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "contact_shop".to_string(),
                parent: "contact".to_string(),
                column_name: "id_contact".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "contact_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "contact_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "image_shop".to_string(),
                parent: "image".to_string(),
                column_name: "id_image".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "image_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "image_shop".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "image_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "image_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "feature_shop".to_string(),
                parent: "feature".to_string(),
                column_name: "id_feature".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "feature_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "feature_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "group_shop".to_string(),
                parent: "group".to_string(),
                column_name: "id_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "group_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "group_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "tax_rules_group_shop".to_string(),
                parent: "tax_rules_group".to_string(),
                column_name: "id_tax_rules_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "tax_rules_group_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "tax_rules_group_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "zone_shop".to_string(),
                parent: "zone".to_string(),
                column_name: "id_zone".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "zone_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "zone_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "manufacturer_shop".to_string(),
                parent: "manufacturer".to_string(),
                column_name: "id_manufacturer".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "manufacturer_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "manufacturer_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "supplier_shop".to_string(),
                parent: "supplier".to_string(),
                column_name: "id_supplier".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "supplier_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "supplier_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "store_shop".to_string(),
                parent: "store".to_string(),
                column_name: "id_store".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "store_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "store_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "module_shop".to_string(),
                parent: "module".to_string(),
                column_name: "id_module".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "module_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "module_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "webservice_account_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "stock".to_string(),
                parent: "warehouse".to_string(),
                column_name: "id_warehouse".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "stock".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "stock".to_string(),
                parent: "product_attribute".to_string(),
                column_name: "id_product_attribute".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "warehouse_shop".to_string(),
                parent: "warehouse".to_string(),
                column_name: "id_warehouse".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "warehouse_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "warehouse_shop".to_string(),
                parent: "warehouse".to_string(),
                column_name: "id_warehouse".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "warehouse_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "warehouse_carrier".to_string(),
                parent: "warehouse".to_string(),
                column_name: "id_warehouse".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "warehouse_carrier".to_string(),
                parent: "carrier".to_string(),
                column_name: "id_carrier".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "warehouse_carrier".to_string(),
                parent: "warehouse".to_string(),
                column_name: "id_warehouse".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "warehouse_carrier".to_string(),
                parent: "carrier".to_string(),
                column_name: "id_carrier".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "stock_available".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "stock_available".to_string(),
                parent: "shop_group".to_string(),
                column_name: "id_shop_group".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "stock_available".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "stock_available".to_string(),
                parent: "product_attribute".to_string(),
                column_name: "id_product_attribute".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "supply_order".to_string(),
                parent: "supplier".to_string(),
                column_name: "id_supplier".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "supply_order".to_string(),
                parent: "warehouse".to_string(),
                column_name: "id_warehouse".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "supply_order_detail".to_string(),
                parent: "supply_order".to_string(),
                column_name: "id_supply_order".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "supply_order_detail".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "supply_order_detail".to_string(),
                parent: "product_attribute".to_string(),
                column_name: "id_product_attribute".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "supply_order_history".to_string(),
                parent: "supply_order".to_string(),
                column_name: "id_supply_order".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "supply_order_history".to_string(),
                parent: "employee".to_string(),
                column_name: "id_employee".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "supply_order_history".to_string(),
                parent: "state".to_string(),
                column_name: "id_state".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "supply_order_receipt_history".to_string(),
                parent: "supply_order_detail".to_string(),
                column_name: "id_supply_order_detail".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "supply_order_receipt_history".to_string(),
                parent: "supply_order_state".to_string(),
                column_name: "id_supply_order_state".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_supplier".to_string(),
                parent: "supplier".to_string(),
                column_name: "id_supplier".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "product_supplier".to_string(),
                parent: "product".to_string(),
                column_name: "id_product".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_carrier".to_string(),
                parent: "order".to_string(),
                column_name: "id_order".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_carrier".to_string(),
                parent: "carrier".to_string(),
                column_name: "id_carrier".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_carrier".to_string(),
                parent: "order_invoice".to_string(),
                column_name: "id_order_invoice".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "risk_lang".to_string(),
                parent: "risk".to_string(),
                column_name: "id_risk".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "risk_lang".to_string(),
                parent: "lang".to_string(),
                column_name: "id_lang".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "risk_lang".to_string(),
                parent: "risk".to_string(),
                column_name: "id_risk".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "category_shop".to_string(),
                parent: "category".to_string(),
                column_name: "id_category".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "category_shop".to_string(),
                parent: "shop".to_string(),
                column_name: "id_shop".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "module_preference".to_string(),
                parent: "employee".to_string(),
                column_name: "id_employee".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_invoice_payment".to_string(),
                parent: "order_payment".to_string(),
                column_name: "id_order_payment".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_invoice_payment".to_string(),
                parent: "order".to_string(),
                column_name: "id_order".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "smarty_cache".to_string(),
                parent: "id".to_string(),
                column_name: "cache_id".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_slip_detail_tax".to_string(),
                parent: "order_slip_detail".to_string(),
                column_name: "id_order_slip_detail".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "order_slip_detail_tax".to_string(),
                parent: "tax".to_string(),
                column_name: "id_tax".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },

            KeyRelationship{
                child: "cms_role".to_string(),
                parent: "cms".to_string(),
                column_name: "id_cms".to_string(),
                parent_child_decorrelation_policy: NoDecorRetain,
                child_parent_decorrelation_policy: NoDecorRetain,
           },
        ],
    }
}
