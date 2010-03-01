// example of tagging an object with a uuid
// see boost/libs/uuid/test/test_tagging.cpp

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <vector>
#include <iomanip>

using namespace std;

void print( vector<char> v )
{
        std::cout.fill( '0' );
        for( int i=0 ; i<v.size( ) ; i++ )
        {
                std::cout << setw(8) << std::hex << (int)v[i];
                std::cout<< "-" ;
        }

        cout << endl;
}



class object
{
public:
    object()
        : tag(boost::uuids::random_generator()())
        , state(0)
    {}
    
    explicit object(int state)
        : tag(boost::uuids::random_generator()())
        , state(state)
    {}
    
    object(object const& rhs)
        : tag(rhs.tag)
        , state(rhs.state)
    {}
    
    bool operator==(object const& rhs) const {
        return tag == rhs.tag;
    }
    
    object& operator=(object const& rhs) {
        tag = rhs.tag;
        state = rhs.state;
    }

    void printMe( )
    {
	std::vector<char> v(tag.size());
	std::copy(tag.begin(), tag.end(), v.begin());
	print( v );
    }
    
    int get_state() const { return state; }
    void set_state(int new_state) { state = new_state; }
    
private:
    boost::uuids::uuid tag;
    
    int state;
};

int main( )
{
	object o1(1);
	o1.printMe();
	object o2 = o1;
	o2.set_state(2);
	assert(o1 == o2);

	object o3(3);
//	assert(o1 != o3);
//	assert(o2 != o3);
}
