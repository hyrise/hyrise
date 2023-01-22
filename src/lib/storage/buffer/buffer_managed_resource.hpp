#include <boost/container/pmr/memory_resource.hpp>

namespace hyrise {

//https://stackoverflow.com/questions/38010544/polymorphic-allocator-when-and-why-should-i-use-it
// http://www.club.cc.cmu.edu/~ajo/disseminate/2018-05-07-slides.pdf
template <typename PointerType>
class BufferManagedResource : public std::pmr::memory_resource {
public:
    PointerType allocate(size_t bytes, size_t align = alignof(max_align_t)) {
        return do_allocate(bytes, align);
    }
    void deallocate(PointerType ptr, size_t bytes, size_t align = alignof(max_align_t)) {
        do_deallocate(ptr, bytes, align);
    }
    bool is_equal(const fancy_memory_resource& rhs) const noexcept {
        return do_is_equal(rhs);
    }
    virtual ~BufferManagedResource() = default;
private:
    virtual PointerType do_allocate(size_t bytes, size_t align) = 0;
    virtual void do_deallocate(PointerType p, size_t bytes, size_t align) = 0;
    virtual bool do_is_equal(const BufferManagedResource& rhs) const noexcept = 0;
};
}