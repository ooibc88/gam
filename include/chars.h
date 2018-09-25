// Copyright (c) 2018 The GAM Authors 



#ifndef CHARS_H
#define CHARS_H

static inline int appendInteger(char* buf) {return 0;}
static inline int readInteger(char* buf) {return 0;}

template<typename Type1, typename ... Types,
    typename = typename std::enable_if<std::is_integral<Type1>::value || std::is_pointer<Type1>::value>::type>
static int appendInteger(char* buf, Type1 value, Types ... values) {
    *(Type1*) buf = value;
    return sizeof(Type1) + appendInteger(buf + sizeof(Type1), values...);
}

template<typename Type1, typename ... Types,
    typename = typename std::enable_if<std::is_integral<Type1>::value || std::is_pointer<Type1>::value>::type>
static int readInteger(char* buf, Type1& value, Types&... values) {
    value = *(Type1*) buf;
    return sizeof(Type1) + readInteger(buf + sizeof(Type1), values...);
}

#endif

