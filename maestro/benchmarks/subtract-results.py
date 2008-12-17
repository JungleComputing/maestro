import sys

if len( sys.argv ) != 5:
    print 'Bad number of parameters'
    sys.exit( 1 )

label_learn = sys.argv[1]
time_learn = float( sys.argv[2] )
label_full = sys.argv[3]
time_full = float( sys.argv[4] )

if label_learn != label_full:
    # Sanity check
    print 'Different labels'
    sys.exit( 1 )

print label_learn, (time_full-time_learn)
