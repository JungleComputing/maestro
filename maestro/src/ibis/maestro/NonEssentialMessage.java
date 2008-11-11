package ibis.maestro;

import ibis.ipl.IbisIdentifier;

abstract class NonEssentialMessage extends Message
{
    /** FIXME. */
    private static final long serialVersionUID = 1L;

    /** For the sender, when to try sending the message again. 
     * For the receiver, the actual moment (according to the sender clock)
     * the message was sent.
     */
    long sendMoment = 0;
    
    /** How many times have we tried to send the message? */
    int tries = 0;

    final IbisIdentifier destination;
    
    NonEssentialMessage( IbisIdentifier destination )
    {
	this.destination = destination;
    }
}
