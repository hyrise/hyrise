#pragma once

#include "abstract_operator.hpp"

namespace opossum {

class get_table : public abstract_operator {
public:
	get_table(const std::string &name);
	virtual void execute();
    virtual std::shared_ptr<table> get_output() const;

protected:
    virtual const std::string get_name() const;
    virtual short get_num_in_tables() const;
    virtual short get_num_out_tables() const;

    const std::string _name;
};

}