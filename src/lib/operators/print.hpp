#pragma once

#include "abstract_operator.hpp"

namespace opossum {

class print : public abstract_operator {
public:
	print(const std::shared_ptr<abstract_operator> in);
	virtual void execute();
    virtual std::shared_ptr<table> get_output() const;

protected:
    virtual const std::string get_name() const;
    virtual short get_num_in_tables() const;
    virtual short get_num_out_tables() const;
};

}