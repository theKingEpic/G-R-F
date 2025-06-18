#include <iostream>
#include <memory>
#include <regex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace pgq2cypher {

// 异常处理类
class ConversionException : public std::runtime_error {
public:
	using std::runtime_error::runtime_error;
};

// 抽象语法树结构
struct NodePattern {
	std::string variable;
	std::vector<std::string> labels;
};

struct RelationshipPattern {
	std::string variable;
	std::string type;
	std::string direction;
};

// 策略接口
class IPatternTokenizer {
public:
	virtual ~IPatternTokenizer() = default;
	virtual std::vector<std::string> tokenize(const std::string &pattern) = 0;
};

class IExpressionConverter {
public:
	virtual ~IExpressionConverter() = default;
	virtual std::string convertWhere(const std::string &expression) = 0;
	virtual std::string convertReturn(const std::string &column) = 0;
};

// 默认实现
class DefaultTokenizer : public IPatternTokenizer {
public:
	std::vector<std::string> tokenize(const std::string &pattern);
};

class DefaultExpressionConverter : public IExpressionConverter {
public:
	std::string convertWhere(const std::string &expression) override {
		return expression;
		// std::regex columnRegex(R"\bT\.(\w+)\b");
		// return std::regex_replace(expression, columnRegex, "$1");
	}

	std::string convertReturn(const std::string &column) override {
		return column;
	}
};

// 核心转换器
class PGQ2CypherConverter {
public:
	PGQ2CypherConverter()
	    : tokenizer_(std::unique_ptr<DefaultTokenizer>(new DefaultTokenizer())),
	      exprConverter_(std::unique_ptr<DefaultExpressionConverter>(new DefaultExpressionConverter())) {
		matchNodeColonMode = std::regex(R"(\((\w+)?(?:\s*:\s*([^\)]+))?\))");
		matchNodeIsMode = std::regex(R"(\((\w+)?\s+IS\s+([^\)]+)\))");
		matchEdgeColonMode = std::regex(R"(\[(\w+)?(?:\s*:\s*([^\]]+))?\])", std::regex::icase);
		matchEdgeIsMode = std::regex(R"(\[(\w+)?\s+IS\s+([^\]]+)\])", std::regex::icase);
	}

	void setTokenizer(std::unique_ptr<IPatternTokenizer> tokenizer) {
		tokenizer_ = std::move(tokenizer);
	}

	void setExpressionConverter(std::unique_ptr<IExpressionConverter> converter) {
		exprConverter_ = std::move(converter);
	}

	std::tuple<std::string, std::vector<std::string>> convert(const std::string &pgq);

private:
	std::regex matchNodeColonMode, matchNodeIsMode, matchEdgeColonMode, matchEdgeIsMode;
	std::unique_ptr<IPatternTokenizer> tokenizer_;
	std::unique_ptr<IExpressionConverter> exprConverter_;

	// 提取GRAPH_TABLE内容
	std::string extractGraphTableContent(const std::string &input);

	// 解析各子句
	std::tuple<std::string, std::string, std::string> parseClauses(const std::string &content);

	// 构建MATCH子句
	std::string buildMatchClause(const std::string &matchClause);

	std::string convertPattern(const std::string &pattern);

	void convertNode(const std::string &node, std::string &result);
	void formatNode(std::string &var, std::string &label, std::string &result);

	void convertRelationship(const std::string &rel, std::string &result);
	void formatEdge(std::string &var, std::string &label, std::string &result);

	std::string convertDirection(const std::string &dir);

	// 构建WHERE子句
	std::string buildWhereClause(const std::string &whereClause);

	// 构建RETURN子句
	std::tuple<std::string, std::vector<std::string>> buildReturnClause(const std::string &columnsClause);
}; 

} // namespace pgq2cypher