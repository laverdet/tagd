CPPFLAGS += -march=native -ggdb -I./threadpool

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $^

tagd: tagd.o libeti_worker.o
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -o $@ $^ -ljson_spirit -lev -ldl -lboost_thread

idfd: idfd.o libeti_worker.o
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -o $@ $^ -O3 -ljson_spirit -lev -ldl -lboost_thread

clean:
	$(RM) *.o
