import re


# 定义解析五级标签规则的函数
def rule_to_tuple(rule):
    start, end = map(int, rule.split("-"))
    return start, end


# 定义解析URL以提取产品ID的函数
def url_to_product(url):
    regex_pattern = r'^.+\/product\/(\d+)\.html.+$'
    match = re.search(regex_pattern, url)
    if match:
        return match.group(1)
    else:
        return "not_a_product"


# 定义提取用户推荐的top5商品的函数
def string_to_product(rule):
    return tuple(map(int, rule.split(",")))
