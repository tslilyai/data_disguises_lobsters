with open("prestashop.sql") as f:
    for line in f.readlines() :
        if "CREATE TABLE" in line:
            childname = line.split("` (")[0]
            childname = childname.split("PREFIX_")[1]
            childname = childname.strip().strip("\`")
        else:
            if "KEY" in line:
                char1 = '('
                char2 = ')'
                keys = line[line.find(char1)+1 : line.find(char2)]
                keys = keys.split(",")
                for key in keys:
                    if "id" in key:
                        parent_col = key.strip().strip('\`')
                        ps = parent_col.split("_")
                        parentname = "_".join(ps[1:])
                        if parentname != childname:
                            print("""
                            KeyRelationship{{
                                child: "{}".to_string(),
                                parent: "{}".to_string(),
                                column_name: "{}".to_string(),
                                parent_child_decorrelation_policy: NoDecorRetain,
                                child_parent_decorrelation_policy: NoDecorRetain,
                           }},""".format(childname, parentname, parent_col))
