#ifdef __cplusplus
extern "C" {
#endif

#include <stdlib.h>
#include <math.h>
#include "common/common.h"
#include "x264.h"

#include "compression_x264.h"

uint8_t *mux_buffer = NULL;
int mux_buffer_size = 0;

x264_param_t param;
x264_t *h;
x264_picture_t pic;

int     i_frame, i_frame_total;
int64_t i_file;
int     i_frame_size;
int     i_progress;

jclass cls = NULL;
jmethodID mid;

jbyteArray buffer = NULL;
int bytes = 0;
int length = 0;

int lookupExchangeMethod(JNIEnv *env, jobject this) 
{ 
	jclass clazz = (*env)->GetObjectClass(env, this); 

	if (clazz == NULL) { 
		return -1;
	} else { 
		cls = (*env)->NewGlobalRef(env, clazz);
	}

	if (cls == NULL) { 
		return -2;
	}	
	
	mid = (*env)->GetMethodID(env, clazz, "exchangeBuffer", "([BIZZ)[B");

	return 0;
}

int freeBuffer(JNIEnv *env, jobject this)
{
	if (buffer != NULL) {
                (*env)->CallObjectMethod(env, this, mid, buffer, bytes, JNI_FALSE);
                (*env)->DeleteGlobalRef(env, buffer);
        }

	return 0;
}

int swapBuffers(JNIEnv *env, jobject this, jboolean needNew, jboolean forced) 
{ 
	jbyteArray tmp = NULL;	

	if (buffer == NULL) { 
		tmp = (*env)->CallObjectMethod(env, this, mid, NULL, 0, needNew, forced);
	} else { 
		tmp = (*env)->CallObjectMethod(env, this, mid, buffer, bytes, needNew, forced);
		(*env)->DeleteGlobalRef(env, buffer);
	}

	if (needNew == JNI_TRUE) {
		if (tmp == NULL) { 
			return -1;
		}

		buffer = (*env)->NewGlobalRef(env, tmp);
		length = (*env)->GetArrayLength(env, buffer);
		bytes = 0;	
	} else { 
		buffer = NULL;
		length = 0;
		bytes = 0;
	}

	return 0;
}

int write_data(JNIEnv *env, jobject this, uint8_t *p_nalu, int i_size )
{
	int left = i_size; 
	int room;
	int offset = 0;

	while (left > 0) 
	{ 
		room = length - bytes;		

		if (room == 0) 
		{ 		
			if (swapBuffers(env, this, JNI_TRUE, JNI_FALSE) != 0) 
			{ 
				fprintf(stderr, "x264 [warn]: write failed!\n");			
				return -1;
			}
		} else if (left <= room) {
			(*env)->SetByteArrayRegion(env, buffer, bytes, left, p_nalu + offset);
			bytes += left;
			left = 0;
		} else { // left > room
			(*env)->SetByteArrayRegion(env, buffer, bytes, room, p_nalu + offset);
			left -= room;
			bytes += room;
			offset += room;
		}
	} 

	return i_size;
}


int Encode_frame(JNIEnv *env, jobject this, x264_picture_t *pic)
{
    x264_picture_t pic_out;
    x264_nal_t *nal;
    int i_nal, i;
    int i_file = 0;

    if( x264_encoder_encode( h, &nal, &i_nal, pic, &pic_out ) < 0 )
    {
        fprintf( stderr, "x264 [error]: x264_encoder_encode failed\n" );
    }

    for( i = 0; i < i_nal; i++ )
    {
        int i_size;

        if( mux_buffer_size < nal[i].i_payload * 3/2 + 4 )
        {
            mux_buffer_size = nal[i].i_payload * 2 + 4;
            x264_free( mux_buffer );
            mux_buffer = x264_malloc( mux_buffer_size );
        }

        i_size = mux_buffer_size;
        x264_nal_encode( mux_buffer, &i_size, 1, &nal[i] );     	
	i_file += write_data( env, this, mux_buffer, i_size );
    }

// Note sure what this does....
//    if (i_nal)
//       p_set_eop( hout, &pic_out );

    return i_file;
}



/*
 * Class:     compression_x264
 * Method:    x264_init
 * Signature: (III)I
 */
JNIEXPORT jint JNICALL Java_compression_x264_x264_1init
  (JNIEnv *env, jobject this, jint width, jint height, jint frames, jint bitrate) 
{ 
	if (lookupExchangeMethod(env, this) != 0) { 
		return -1;
	}

	i_frame = 0;
	i_file = 0;
	i_frame_total = 0;

	x264_param_default( &param );

        param.i_frame_total = frames;
	param.i_width = width;
	param.i_height = height;
	param.i_threads = 1;

	param.rc.i_bitrate = bitrate;
	param.rc.i_rc_method = X264_RC_ABR;

   	if( ( h = x264_encoder_open( &param ) ) == NULL )
	{
        	fprintf( stderr, "x264 [error]: x264_encoder_open failed\n");
//	        p_close_infile( opt->hin );
//              p_close_outfile( opt->hout );
        	return -1;
        }

fprintf( stderr, "x264 [info]: width %d height %d h = %p\n", param.i_width, param.i_height, h);

        /* Create a new pic */
        x264_picture_alloc( &pic, X264_CSP_I420, param.i_width, param.i_height);

//        i_start = x264_mdate();

	return 0;	
}

/*
 * Class:     compression_x264
 * Method:    x264_add_frame
 * Signature: ([B[B[B)I
 */
JNIEXPORT jint JNICALL Java_compression_x264_x264_1add_1frame
  (JNIEnv *env, jobject this, jbyteArray y, jbyteArray u, jbyteArray v)
{
	// Copy the frame data from the parameters

	int bytes = param.i_width * param.i_height;

	(*env)->GetByteArrayRegion(env, y, 0, param.i_width * param.i_height, pic.img.plane[0]);
	(*env)->GetByteArrayRegion(env, u, 0, param.i_width * param.i_height / 4 , pic.img.plane[1]);
	(*env)->GetByteArrayRegion(env, v, 0, param.i_width * param.i_height / 4 , pic.img.plane[2]);

//	fprintf(stderr, "Copied 2x%d bytes\n", bytes);

        pic.i_pts = (int64_t) i_frame * param.i_fps_den;

	/* Do not force any parameters */
        pic.i_type = X264_TYPE_AUTO;
        pic.i_qpplus1 = 0;

        i_file += Encode_frame( env, this, &pic );
        i_frame++;

  	return 0;	
}

/*
 * Class:     compression_x264
 * Method:    x264_flush
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_compression_x264_x264_1flush
  (JNIEnv *env, jobject this)
{
        /* Flush delayed B-frames */
        do {
                i_file += i_frame_size = Encode_frame( env, this, NULL );
        } while( i_frame_size );


	if (swapBuffers(env, this, JNI_TRUE, JNI_TRUE) != 0)
        {
        	fprintf(stderr, "x264 [warn]: flush failed!\n");
                return -1;
        }

        return 0;
}

/*
 * Class:     compression_x264
 * Method:    x264_done
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_compression_x264_x264_1done
  (JNIEnv *env, jobject this) 
{
	/* Flush delayed B-frames */
    	do {
       		i_file += i_frame_size = Encode_frame( env, this, NULL );
    	} while( i_frame_size );

	if (swapBuffers(env, this, JNI_FALSE, JNI_TRUE) != 0)
        {
        	fprintf(stderr, "x264 [warn]: flush failed!\n");
        }

    	x264_picture_clean( &pic );
    	x264_encoder_close( h );

//	freeBuffer(env, this);

	return 0;
}




#ifdef __cplusplus
}
#endif
