//#############################
//# Copyright 2016 Otmar Ertl #
//#############################
// https://github.com/oertl/hyperloglog-sketch-estimation-paper/blob/master/c%2B%2B/two_hyperloglog_statistic.hpp

#ifndef _TWO_HYPERLOGLOG_STATISTIC_HPP_
#define _TWO_HYPERLOGLOG_STATISTIC_HPP_

#include <string>
#include <sstream>
#include <vector>
#include <algorithm>
#include <cassert>
#include <numeric>
#include <cmath>

class TwoHyperLogLogStatistic {

    const std::vector<int> equal;
    const std::vector<int> smaller1;
    const std::vector<int> smaller2;
    const std::vector<int> larger1;
    const std::vector<int> larger2;

    int getSize() const {
        return static_cast<int>(equal.size());
    }

public:


    TwoHyperLogLogStatistic(
        const std::vector<int>& equal_,
        const std::vector<int>& smaller1_,
        const std::vector<int>& smaller2_,
        const std::vector<int>& larger1_,
        const std::vector<int>& larger2_) :

        equal(equal_),
        smaller1(smaller1_),
        smaller2(smaller2_),
        larger1(larger1_),
        larger2(larger2_)
    {

        assert(smaller1_.size() == equal_.size());
        assert(smaller2_.size() == equal_.size());
        assert(larger1_.size() == equal_.size());
        assert(larger2_.size() == equal_.size());

    }

    const std::vector<int>& getSmaller1Counts() const {
        return smaller1;
    }

    const std::vector<int>& getSmaller2Counts() const {
        return smaller2;
    }

    const std::vector<int>& getLarger1Counts() const {
        return larger1;
    }

    const std::vector<int>& getLarger2Counts() const {
        return larger2;
    }

    const std::vector<int>& getEqualCounts() const {
        return equal;
    }

    const std::vector<int> get1Counts() const {
        const int size = getSize();
        std::vector<int> result(size);
        for (int i = 0; i < size; ++i) {
            result[i] = equal[i] + smaller1[i] + larger1[i];
        }
        return result;
    }

    const std::vector<int> get2Counts() const {
        const int size = getSize();
        std::vector<int> result(size);
        for (int i = 0; i < size; ++i) {
            result[i] = equal[i] + smaller2[i] + larger2[i];
        }
        return result;
    }

    const std::vector<int> getMaxCounts() const {
        const int size = getSize();
        std::vector<int> result(size);
        for (int i = 0; i < size; ++i) {
            result[i] = equal[i] + larger1[i] + larger2[i];
        }
        return result;
    }

    const std::vector<int> getMinCounts() const {
        const int size = getSize();
        std::vector<int> result(size);
        for (int i = 0; i < size; ++i) {
            result[i] = equal[i] + smaller1[i] + smaller2[i];
        }
        return result;
    }

     int getSmaller1Count(const std::size_t idx) const {
        return smaller1[idx];
    }

     int getSmaller2Count(const std::size_t idx) const {
        return smaller2[idx];
    }

     int getLarger1Count(const std::size_t idx) const {
        return larger1[idx];
    }

     int getLarger2Count(const std::size_t idx) const {
        return larger2[idx];
    }

     int getEqualCount(const std::size_t idx) const {
        return equal[idx];
    }

     int get1Count(const std::size_t idx) const {
        return smaller1[idx] + equal[idx] + larger1[idx];
    }

     int get2Count(const std::size_t idx) const {
        return smaller2[idx] + equal[idx] + larger2[idx];
    }

     int getMaxCount(const std::size_t idx) const {
        return larger1[idx] + equal[idx] + larger2[idx];
    }

     int getMinCount(const std::size_t idx) const {
        return smaller1[idx] + equal[idx] + smaller2[idx];
    }

    int getNumRegisters() const {
        return
            std::accumulate(equal.begin(), equal.end(), 0) +
            std::accumulate(smaller1.begin(), smaller1.end(), 0) +
            std::accumulate(smaller2.begin(), smaller2.end(), 0);
    }

    int getP() const {
        int m = getNumRegisters();
        int p;
        std::frexp(m, &p);
        p -= 1;
        assert(m == 1 << p);
        return p;
    }

    int getQ() const {
        return getSize() - 2;
    }

    std::string toString() const {

        std::stringstream ss;

        for (int i : equal) ss << i << " ";
        for (int i : larger2) ss << i << " ";
        for (int i : smaller1) ss << i << " ";
        for (int i : smaller2) ss << i << " ";
        for (int i : larger1) ss << i << " ";

        return ss.str();
    }

    static TwoHyperLogLogStatistic fromString(const std::string& s) {

        std::stringstream stream(s);
        int i;
        std::vector<int> v;
        while(stream >> i) {
            v.push_back(i);
        }

        assert(v.size() % 5 == 0);
        int size = v.size()/5;

        std::vector<int> equal(v.begin() + 0*size, v.begin() + 1*size);
        std::vector<int> larger2(v.begin() + 1*size, v.begin() + 2*size);
        std::vector<int> smaller1(v.begin() + 2*size, v.begin() + 3*size);
        std::vector<int> smaller2(v.begin() + 3*size, v.begin() + 4*size);
        std::vector<int> larger1(v.begin() + 4*size, v.begin() + 5*size);

        return TwoHyperLogLogStatistic(equal, smaller1, smaller2, larger1, larger2);
    }

};

#endif // _TWO_HYPERLOGLOG_STATISTIC_HPP_