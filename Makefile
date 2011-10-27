CPPFLAGS += -ggdb

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $^

tagd: tagd.o libeti_worker.o
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -o $@ $^ -ljson_spirit -lev

clean:
	$(RM) *.o
