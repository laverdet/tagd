CPPFLAGS += -ggdb -I./threadpool -I./backtrace

%.o: %.cc
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $^

tagd: tagd.o libeti_worker.o
	$(CXX) $(CXXFLAGS) $(CPPFLAGS) -o $@ $^ ./backtrace/libs/backtrace/src/backtrace.cpp -ljson_spirit -lev -ldl -lboost_thread

clean:
	$(RM) *.o
