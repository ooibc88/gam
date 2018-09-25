// Copyright (c) 2018 The GAM Authors 

#ifndef LINUX_KERNEL_H_
#define LINUX_KERNEL_H_

/*---------------------------------------------------------------------------*/
/* defines								     */
/*---------------------------------------------------------------------------*/
//#ifndef min
//#define min(a, b) (((a) < (b)) ? (a) : (b))
//#endif
//
//#ifndef max
//#define max(a, b) (((a) < (b)) ? (b) : (a))
//#endif
#define likely(x)		__builtin_expect(!!(x), 1)
#define unlikely(x)		__builtin_expect(!!(x), 0)

#define __ALIGN_MASK(x,mask)    (((x)+(mask))&~(mask))
#define ALIGN(x,a)              __ALIGN_MASK(x,(a)-1)

#ifndef roundup
# define roundup(x, y)  ((((x) + ((y) - 1)) / (y)) * (y))
#endif /* !defined(roundup) */

//#ifndef offsetof
//#ifdef __compiler_offsetof
//#define offsetof(TYPE, MEMBER) __compiler_offsetof(TYPE, MEMBER)
//#else
//#define offsetof(TYPE, MEMBER) ((size_t) &((TYPE *)0)->MEMBER)
//#endif
//#endif
//
///**
// * container_of - cast a member of a structure out to the containing structure
// * @ptr:	the pointer to the member.
// * @type:	the type of the container struct this is embedded in.
// * @member:	the name of the member within the struct.
// *
// */
//#ifndef container_of
//#define container_of(ptr, type, member) ({			\
//	const typeof(((type *)0)->member) * __mptr = (ptr);	\
//	(type *)((char *)__mptr - offsetof(type, member)); })
//#endif
//
///*
// *	These inlines deal with timer wrapping correctly. You are
// *	strongly encouraged to use them
// *	1. Because people otherwise forget
// *	2. Because if the timer wrap changes in future you won't have to
// *	   alter your driver code.
// *
// * time_after(a,b) returns true if the time a is after time b,
// * where a and b are jiffies since the boot time
// */
//#define time_after(a,b)		\
//	 ((long)((b) - (a)) < 0)
//#define time_before(a,b)	time_after(b,a)
//
//#define time_after_eq(a,b)	\
//	 ((long)((a) - (b)) >= 0)
//#define time_before_eq(a,b)	time_after_eq(b,a)
//
///*
// * Calculate whether a is in the range of [b, c].
// */
//#define time_in_range(a,b,c) \
//	(time_after_eq(a,b) && \
//	 time_before_eq(a,c))
#endif
