package ibis.maestro;

abstract class NonEssentialMessage extends Message
{
    /** For the sender, when to try sending the message again. 
     * For the receiver, the actual moment (according to the sender clock)
     * the message was sent.
     */
    long sendMoment = 0;
    
    /** How many times have we tried to send the message? */
    int tries = 0;
    
}
