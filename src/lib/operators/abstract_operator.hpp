#pragma once

#include "../common.hpp"
#include "../storage/table.hpp"

namespace opossum {

class abstract_operator {
public:
    abstract_operator(const std::shared_ptr<abstract_operator> left = nullptr, const std::shared_ptr<abstract_operator> right = nullptr);
    abstract_operator(abstract_operator const&) = delete;
    abstract_operator(abstract_operator&&) = default;

    virtual void execute() = 0;
    virtual std::shared_ptr<table> get_output() const = 0;

protected:
    virtual const std::string get_name() const = 0;
    virtual short get_num_in_tables() const = 0;
    virtual short get_num_out_tables() const = 0;

    const std::shared_ptr<table> _input_left, _input_right;
};

class abstract_operator_impl {
public:
    virtual void execute() = 0;
    virtual std::shared_ptr<table> get_output() const = 0;
};

}