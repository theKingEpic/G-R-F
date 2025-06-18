#include "relgo_statement.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

namespace duckdb {

RelGoStatement::RelGoStatement(const RelGoStatement &other) : SQLStatement(other), node(other.node->Copy()) {
}

unique_ptr<SQLStatement> RelGoStatement::Copy() const {
	return unique_ptr<RelGoStatement>(new RelGoStatement(*this));
}

bool RelGoStatement::Equals(const SQLStatement &other_p) const {
	if (type != other_p.type) {
		return false;
	}
	auto &other = other_p.Cast<RelGoStatement>();
	return node->Equals(other.node.get());
}

string RelGoStatement::ToString() const {
	return node->ToString();
}

} // namespace duckdb
