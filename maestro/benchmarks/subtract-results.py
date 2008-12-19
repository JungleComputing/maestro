import sys
import string

if len( sys.argv ) != 6:
    print 'Bad number of parameters: %d instead of 4' % len( sys.argv )
    print 'Parameters: [' + string.join( sys.argv, ',' ) + ']'
    sys.exit( 1 )

label_learn = sys.argv[1]
time_learn = float( sys.argv[2] )
label_full = sys.argv[3]
time_full = float( sys.argv[4] )
output_file = sys.argv[5]

if label_learn != label_full:
    # Sanity check
    print 'Different labels'
    sys.exit( 1 )

fhnd = open( output_file, 'w' )
fhnd.write( "%s %f\n" % (label_learn, (time_full-time_learn)) )
fhnd.close()
