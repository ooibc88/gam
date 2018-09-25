// Copyright (c) 2018 The GAM Authors 

#ifndef INCLUDE_UTIL_H_
#define INCLUDE_UTIL_H_

#include <vector>
#include <sstream>
#include "settings.h"
#include "structure.h"

template <class T>
inline vector<T>& Split(stringstream& ss, vector<T>& elems, char delim) {
	T item;
	while (ss >> item) {
		elems.push_back(item);
		if(ss.peek() == delim) ss.ignore();
	}
	return elems;
}

template <class T>
inline vector<T>& Split(string& s, vector<T>& elems, char delim = DEFAULT_SPLIT_CHAR) {
	stringstream ss(s);
	return Split(ss, elems, delim);
}

template <class T>
inline vector<T>& Split(char *s, vector<T>& elems, char delim = DEFAULT_SPLIT_CHAR) {
	stringstream ss(s);
	return Split(ss, elems, delim);
}

template <>
vector<string>& Split<string>(stringstream& ss, vector<string>& elems, char delim);

string get_local_ip(const char* iface = nullptr);
inline string get_local_ip(const string iface = "") {return get_local_ip(iface.empty() ? nullptr : iface.c_str());}

long get_time();

#define atomic_add(v, i) __sync_fetch_and_add((v), (i))
#define atomic_read(v) __sync_fetch_and_add((v), (0))

#endif /* INCLUDE_UTIL_H_ */
