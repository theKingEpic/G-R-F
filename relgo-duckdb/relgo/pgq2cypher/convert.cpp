#include "convert.hpp"

namespace pgq2cypher {

std::vector<std::string> DefaultTokenizer::tokenize(const std::string &pattern) {
	std::vector<std::string> tokens;

	// 修正后的正则表达式
	std::regex patternRegex(R"(\([^\)]*\)|,|-[^-]+->|<-[^-]+-|-[^-]+-)", std::regex::ECMAScript | std::regex::nosubs);

	std::sregex_iterator it(pattern.begin(), pattern.end(), patternRegex);
	std::sregex_iterator end;

	while (it != end) {
		// 过滤空匹配
		if (!it->str().empty()) {
			// 清理逗号周围的空白
			if (it->str() == ",") {
				tokens.push_back(",");
			} else {
				// 添加非逗号token
				tokens.push_back(it->str());
			}
		}
		++it;
	}

	return tokens;
}

std::tuple<std::string, std::vector<std::string>> PGQ2CypherConverter::convert(const std::string &pgq) {
	try {
		std::string graphTableContent = extractGraphTableContent(pgq);

		auto [matchClause, whereClause, columnsClause] = parseClauses(graphTableContent);

		std::string cypher = buildMatchClause(matchClause);
		cypher += buildWhereClause(whereClause);
		auto [return_expr, return_cols] = buildReturnClause(columnsClause);
		cypher += return_expr;

		return {cypher, return_cols};
	} catch (const std::exception &e) {
		throw ConversionException("Conversion failed: " + std::string(e.what()));
	}
}

std::string PGQ2CypherConverter::extractGraphTableContent(const std::string &input) {
	std::regex graphTableRegex(R"(GRAPH_TABLE\s*\(\s*(.*?COLUMNS\s*\(.*\))\s*\)\s*)",
	                           std::regex::icase | std::regex::ECMAScript);
	std::smatch match;

	if (!std::regex_search(input, match, graphTableRegex)) {
		throw ConversionException("Missing GRAPH_TABLE clause");
	}

	return match[1];
}

// 解析各子句
std::tuple<std::string, std::string, std::string> PGQ2CypherConverter::parseClauses(const std::string &content) {
	std::regex matchRegex(R"(MATCH\s*((?:.|\n)*?)(?=\s+WHERE|COLUMNS\b))", std::regex::icase);
	// std::regex matchRegex(R"(MATCH\s*([\s\S]*)(WHERE|COLUMNS|$))", std::regex::icase);
	std::regex whereRegex(R"(WHERE\s*((?:.|\n)*?)(?=\s+COLUMNS\b))", std::regex::icase);
	// std::regex whereRegex(R"(WHERE\s*([\s\S]*)(COLUMNS|$))", std::regex::icase);
	// std::regex columnsRegex(R"(COLUMNS\s*(\((.|\n)*?\)))", std::regex::icase);
	// std::regex columnsRegex(R"(COLUMNS\s*([\s\S]*))", std::regex::icase);

	std::regex columnsRegex(R"(COLUMNS\s*\((([^\(\)]|\(.*\))*)\))", std::regex::icase);

	std::smatch match;
	std::string matchClause, whereClause, columnsClause;

	if (std::regex_search(content, match, matchRegex)) {
		matchClause = match[1];
	}

	if (std::regex_search(content, match, whereRegex)) {
		whereClause = match[1];
	}

	if (std::regex_search(content, match, columnsRegex)) {
		columnsClause = match[1];
	}

	return {matchClause, whereClause, columnsClause};
}

// 构建MATCH子句
std::string PGQ2CypherConverter::buildMatchClause(const std::string &matchClause) {
	std::vector<std::string> patterns = tokenizer_->tokenize(matchClause);
	std::string cypher = "MATCH ";

	for (const auto &pattern : patterns) {
		if (pattern == ",")
			cypher += ", ";
		else
			cypher += convertPattern(pattern);
	}

	return cypher;
}

std::string PGQ2CypherConverter::convertPattern(const std::string &pattern) {
	std::regex nodeRegex(R"(\(([^\)]*)\))");
	std::regex relRegex(R"(\[([^\]]*)\])");
	std::regex directionRegex(R"((->|<-|--))");

	std::string converted = "";
	std::sregex_iterator it(pattern.begin(), pattern.end(), nodeRegex);

	if (it != std::sregex_iterator()) {
		convertNode(it->str(), converted);
	} else {
		std::smatch relMatch;
		if (std::regex_search(pattern, relMatch, relRegex)) {
			converted.append(pattern.begin(), relMatch[0].first);
			// Convert the matched text and append
			convertRelationship(relMatch.str(), converted);
			// Append text after the match
			converted.append(relMatch.suffix().first, pattern.end());
		}
	}

	return converted;
}

void PGQ2CypherConverter::convertNode(const std::string &node, std::string &result) {
	// 同时匹配两种语法：
	// PGQ格式：(p IS Person|Company)
	// Cypher格式：(p:Person:Company)
	std::smatch match;
	std::string var = "";
	std::string labels = "";

	if (std::regex_match(node, match, matchNodeColonMode)) {
		if (match[1].matched || match[2].matched) {
			if (match[1].matched)
				var = match[1];
			if (match[2].matched)
				labels = match[2];
		}

		formatNode(var, labels, result);
		return;
	}

	if (std::regex_match(node, match, matchNodeIsMode)) {
		if (match[1].matched || match[2].matched) {
			if (match[1].matched)
				var = match[1];
			if (match[2].matched)
				labels = match[2];
		}

		formatNode(var, labels, result);
		return;
	}
}

void PGQ2CypherConverter::formatNode(std::string &var, std::string &labels, std::string &result) {
	result += "(" + var;
	if (!labels.empty()) {
		// 替换分隔符：Person|Company -> :Person:Company
		std::string formattedLabels = std::regex_replace(labels, std::regex(R"(\||\s+)"), ":");
		result += ":" + formattedLabels;
	}
	result += ")";
}

void PGQ2CypherConverter::convertRelationship(const std::string &rel, std::string &result) {
	// 同时匹配两种关系语法：
	// PGQ格式：[IS type] 或 [var IS type]
	// Cypher格式：[:TYPE] 或 [var:TYPE]
	std::smatch match;
	std::string var = "";
	std::string type = "";

	if (std::regex_match(rel, match, matchEdgeColonMode)) {
		if (match[1].matched || match[2].matched) {
			if (match[1].matched)
				var = match[1];
			if (match[2].matched)
				type = match[2];
		}

		formatEdge(var, type, result);
		return;
	}

	if (std::regex_match(rel, match, matchEdgeIsMode)) {
		if (match[1].matched || match[2].matched) {
			if (match[1].matched)
				var = match[1];
			if (match[2].matched)
				type = match[2];
		}

		formatEdge(var, type, result);
		return;
	}
}

void PGQ2CypherConverter::formatEdge(std::string &var, std::string &type, std::string &result) {
	// 清洗类型中的多余空格
	type = std::regex_replace(type, std::regex(R"(\s+)"), "");

	// 验证关系类型格式（Cypher仅支持单一类型）
	if (type.find('|') != std::string::npos) {
		throw ConversionException("Multi-type relationships not supported");
	}

	// 构建Cypher关系表达式
	result += "[";
	if (!var.empty()) {
		result += var;
	}
	if (!type.empty()) {
		result += ":" + type;
	}
	result += "]";
}

std::string PGQ2CypherConverter::convertDirection(const std::string &dir) {
	if (dir == "->")
		return "->";
	if (dir == "<-")
		return "<-";
	return "-";
}

// 构建WHERE子句
std::string PGQ2CypherConverter::buildWhereClause(const std::string &whereClause) {
	if (whereClause.empty())
		return "";
	return " WHERE " + exprConverter_->convertWhere(whereClause);
}

// 构建RETURN子句
std::tuple<std::string, std::vector<std::string>>
PGQ2CypherConverter::buildReturnClause(const std::string &columnsClause) {
	std::regex columnRegex(R"(\s*\(?([^\),]+)(?:,\s*([^\)]+))?\)?)");
	std::regex aliasRegex(R"((?:^|\s)AS\s+([^\s,]+))", std::regex_constants::icase);
	std::smatch match;
	std::string returns = "";
	std::vector<std::string> return_names;
	std::string current = "";

	for (size_t i = 0; i < columnsClause.size(); ++i) {
		// if (columnsClause[i] == '(' || columnsClause[i] == ')')
		// 	continue;
		// else 
		if (columnsClause[i] != ',') {
			current += columnsClause[i];
		} else {
			returns += current + ",";
			std::smatch alias_match;
			std::string alias = "";

			if (std::regex_search(current, alias_match, aliasRegex)) {
				if (alias_match.size() > 1) {
					alias = alias_match[1].str();
					return_names.push_back(alias);
				}
			}

			current = "";
		}
	}

	if (!current.empty()) {
		returns += current;
		std::smatch alias_match;
		std::string alias = "";

		if (std::regex_search(current, alias_match, aliasRegex)) {
			if (alias_match.size() > 1) {
				alias = alias_match[1].str();
				return_names.push_back(alias);
			}
		}
	}

	return {" RETURN " + returns, return_names};
	//   throw ConversionException("Invalid COLUMNS clause");
}

} // namespace pgq2cypher