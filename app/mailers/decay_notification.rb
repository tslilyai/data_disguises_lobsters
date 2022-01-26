class DecayNotification < ApplicationMailer
  def notify(user, locator, did)
    @loc = locator
    @did = did 

    mail(
      :to => user.email,
      :subject => "[#{Rails.application.name}] Your acount has been deleted"
    )
  end
end
